package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keegancsmith/sqlf"
	"github.com/lib/pq"
	"github.com/sourcegraph/sourcegraph/cmd/frontend/types"
	"github.com/sourcegraph/sourcegraph/pkg/conf"
	"github.com/sourcegraph/sourcegraph/pkg/db/dbconn"
	"github.com/sourcegraph/sourcegraph/pkg/db/dbutil"
	"github.com/sourcegraph/sourcegraph/pkg/jsonc"
	"github.com/sourcegraph/sourcegraph/schema"
	log15 "gopkg.in/inconshreveable/log15.v2"
)

type externalServices struct{}

// ExternalServicesListOptions contains options for listing external services.
type ExternalServicesListOptions struct {
	Kind string
	*LimitOffset
}

func (o ExternalServicesListOptions) sqlConditions() []*sqlf.Query {
	conds := []*sqlf.Query{sqlf.Sprintf("deleted_at IS NULL")}
	if o.Kind != "" {
		conds = append(conds, sqlf.Sprintf("kind=%s", o.Kind))
	}
	return conds
}

func validateConfig(config string) error {
	// All configs must be valid JSON.
	// If this requirement is ever changed, you will need to update
	// serveExternalServiceConfigs to handle this case.
	_, err := jsonc.Parse(config)
	return err
}

// Create creates a external service.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) Create(ctx context.Context, externalService *types.ExternalService) error {
	if err := validateConfig(externalService.Config); err != nil {
		return err
	}

	externalService.CreatedAt = time.Now()
	externalService.UpdatedAt = externalService.CreatedAt

	return dbconn.Global.QueryRowContext(
		ctx,
		"INSERT INTO external_services(kind, display_name, config, created_at, updated_at) VALUES($1, $2, $3, $4, $5) RETURNING id",
		externalService.Kind, externalService.DisplayName, externalService.Config, externalService.CreatedAt, externalService.UpdatedAt,
	).Scan(&externalService.ID)
}

// ExternalServiceUpdate contains optional fields to update.
type ExternalServiceUpdate struct {
	DisplayName *string
	Config      *string
}

// Update updates a external service.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) Update(ctx context.Context, id int64, update *ExternalServiceUpdate) error {
	if update.Config != nil {
		if err := validateConfig(*update.Config); err != nil {
			return err
		}
	}

	execUpdate := func(ctx context.Context, tx *sql.Tx, update *sqlf.Query) error {
		q := sqlf.Sprintf("UPDATE external_services SET %s, updated_at=now() WHERE id=%d AND deleted_at IS NULL", update, id)
		res, err := tx.ExecContext(ctx, q.Query(sqlf.PostgresBindVar), q.Args()...)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return externalServiceNotFoundError{id: id}
		}
		return nil
	}
	return dbutil.Transaction(ctx, dbconn.Global, func(tx *sql.Tx) error {
		if update.DisplayName != nil {
			if err := execUpdate(ctx, tx, sqlf.Sprintf("display_name=%s", update.DisplayName)); err != nil {
				return err
			}
		}
		if update.Config != nil {
			if err := execUpdate(ctx, tx, sqlf.Sprintf("config=%s", update.Config)); err != nil {
				return err
			}
		}
		return nil
	})
}

type externalServiceNotFoundError struct {
	id int64
}

func (e externalServiceNotFoundError) Error() string {
	return fmt.Sprintf("external service not found: %v", e.id)
}

func (e externalServiceNotFoundError) NotFound() bool {
	return true
}

// Delete deletes an external service.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (*externalServices) Delete(ctx context.Context, id int64) error {
	res, err := dbconn.Global.ExecContext(ctx, "UPDATE external_services SET deleted_at=now() WHERE id=$1 AND deleted_at IS NULL", id)
	if err != nil {
		return err
	}
	nrows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if nrows == 0 {
		return externalServiceNotFoundError{id: id}
	}
	return nil
}

// GetByID returns the external service for id.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) GetByID(ctx context.Context, id int64) (*types.ExternalService, error) {
	conds := []*sqlf.Query{sqlf.Sprintf("id=%d", id)}
	externalServices, err := c.list(ctx, conds, nil)
	if err != nil {
		return nil, err
	}
	if len(externalServices) == 0 {
		return nil, fmt.Errorf("external service not found: id=%d", id)
	}
	return externalServices[0], nil
}

// List returns all external services.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) List(ctx context.Context, opt ExternalServicesListOptions) ([]*types.ExternalService, error) {
	return c.list(ctx, opt.sqlConditions(), opt.LimitOffset)
}

// listConfigs decodes the list configs into result.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) listConfigs(ctx context.Context, kind string, result interface{}) error {
	services, err := c.List(ctx, ExternalServicesListOptions{Kind: kind})
	if err != nil {
		return err
	}
	var configs []json.RawMessage
	for _, service := range services {
		configs = append(configs, json.RawMessage(service.Config))
	}
	buf, err := json.Marshal(configs)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, result)
}

// ListGitHubConnections returns a list of GitHubConnection configs.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) ListGitHubConnections(ctx context.Context) ([]*schema.GitHubConnection, error) {
	if !conf.ExternalServicesEnabled() {
		return conf.Get().Github, nil
	}

	var connections []*schema.GitHubConnection
	if err := c.listConfigs(ctx, "GITHUB", &connections); err != nil {
		return nil, err
	}
	return connections, nil
}

// ListGitLabConnections returns a list of GitLabConnection configs.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) ListGitLabConnections(ctx context.Context) ([]*schema.GitLabConnection, error) {
	if !conf.ExternalServicesEnabled() {
		return conf.Get().Gitlab, nil
	}

	var connections []*schema.GitLabConnection
	if err := c.listConfigs(ctx, "GITLAB", &connections); err != nil {
		return nil, err
	}
	return connections, nil
}

// ListPhabricatorConnections returns a list of PhabricatorConnection configs.
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) ListPhabricatorConnections(ctx context.Context) ([]*schema.PhabricatorConnection, error) {
	if !conf.ExternalServicesEnabled() {
		return conf.Get().Phabricator, nil
	}

	var connections []*schema.PhabricatorConnection
	if err := c.listConfigs(ctx, "PHABRICATOR", &connections); err != nil {
		return nil, err
	}
	return connections, nil
}

// migrateOnce ensures that the migration is only attempted
// once per frontend instance (to avoid unnecessary queries).
var migrateOnce sync.Once

// migrateJsonConfigToExternalServices performs a one time migration to populate
// the new external_services database table with relavant entries in the site config.
// It is idempotent.
//
// This migration can be deleted as soon as (whichever happens first):
//   - All customers have updated to 3.0 or newer.
//   - 3 months after 3.0 is released.
func (c *externalServices) migrateJsonConfigToExternalServices(ctx context.Context) {
	if !conf.ExternalServicesEnabled() {
		return
	}

	migrateOnce.Do(func() {
		// Run in a transaction because we are racing with other frontend replicas.
		err := dbutil.Transaction(ctx, dbconn.Global, func(tx *sql.Tx) error {
			now := time.Now()

			// Attempt to insert a fake config into the DB with id 0.
			// This will fail if the migration has already run.
			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO external_services(id, kind, display_name, config, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7)",
				0, "migration", "", "{}", now, now, now,
			); err != nil {
				return err
			}

			migrate := func(config interface{}, name string) error {
				// Marshaling and unmarshaling is a lazy way to get around
				// Go's lack of covariance for slice types.
				buf, err := json.Marshal(config)
				if err != nil {
					return err
				}
				var configs []interface{}
				if err := json.Unmarshal(buf, &configs); err != nil {
					return nil
				}

				for i, config := range configs {
					jsonConfig, err := json.MarshalIndent(config, "", "  ")
					if err != nil {
						return err
					}

					kind := strings.ToUpper(name)
					displayName := fmt.Sprintf("Migrated %s %d", name, i+1)
					if _, err := tx.ExecContext(
						ctx,
						"INSERT INTO external_services(kind, display_name, config, created_at, updated_at) VALUES($1, $2, $3, $4, $5)",
						kind, displayName, string(jsonConfig), now, now,
					); err != nil {
						return err
					}
				}
				return nil
			}

			if err := migrate(conf.Get().AwsCodeCommit, "AWSCodeCommit"); err != nil {
				return err
			}

			if err := migrate(conf.Get().BitbucketServer, "BitbucketServer"); err != nil {
				return err
			}

			if err := migrate(conf.Get().Github, "GitHub"); err != nil {
				return err
			}

			if err := migrate(conf.Get().Gitlab, "GitLab"); err != nil {
				return err
			}

			if err := migrate(conf.Get().Gitolite, "Gitolite"); err != nil {
				return err
			}

			if err := migrate(conf.Get().Phabricator, "Phabricator"); err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			if pqErr, ok := err.(*pq.Error); ok {
				if pqErr.Constraint == "external_services_pkey" {
					// This is expected when multiple frontend attempt to migrate concurrently.
					// Only one will win.
					return
				}
			}
			log15.Error("migrate transaction failed", "err", err)
		}
	})
}

func (c *externalServices) list(ctx context.Context, conds []*sqlf.Query, limitOffset *LimitOffset) ([]*types.ExternalService, error) {
	c.migrateJsonConfigToExternalServices(ctx)
	q := sqlf.Sprintf(`
		SELECT id, kind, display_name, config, created_at, updated_at
		FROM external_services
		WHERE (%s)
		ORDER BY id DESC
		%s`,
		sqlf.Join(conds, ") AND ("),
		limitOffset.SQL(),
	)

	rows, err := dbconn.Global.QueryContext(ctx, q.Query(sqlf.PostgresBindVar), q.Args()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*types.ExternalService
	for rows.Next() {
		var h types.ExternalService
		if err := rows.Scan(&h.ID, &h.Kind, &h.DisplayName, &h.Config, &h.CreatedAt, &h.UpdatedAt); err != nil {
			return nil, err
		}
		results = append(results, &h)
	}
	return results, nil
}

// Count counts all access tokens that satisfy the options (ignoring limit and offset).
//
// 🚨 SECURITY: The caller must ensure that the actor is a site admin.
func (c *externalServices) Count(ctx context.Context, opt ExternalServicesListOptions) (int, error) {
	q := sqlf.Sprintf("SELECT COUNT(*) FROM external_services WHERE (%s)", sqlf.Join(opt.sqlConditions(), ") AND ("))
	var count int
	if err := dbconn.Global.QueryRowContext(ctx, q.Query(sqlf.PostgresBindVar), q.Args()...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
