import { HoveredToken, LOADER_DELAY } from '@sourcegraph/codeintellify'
import * as H from 'history'
import { combineLatest, merge, Observable, of, Subscription, Unsubscribable } from 'rxjs'
import { catchError, delay, filter, first, map, share, startWith, switchMap, takeUntil } from 'rxjs/operators'
import { ActionItemProps } from '../actions/ActionItem'
import { Services } from '../api/client/services'
import { ContributableMenu, TextDocumentPositionParams } from '../api/protocol'
import { getContributedActionItems } from '../contributions/contributions'
import { ExtensionsControllerProps } from '../extensions/controller'
import { asError, ErrorLike, isErrorLike } from '../util/errors'
import { AbsoluteRepoFilePosition, makeRepoURI, parseRepoURI, toPrettyBlobURL } from '../util/url'
import { HoverContext } from './HoverOverlay'

const LOADING: 'loading' = 'loading'

/**
 * This function is passed to {@link module:@sourcegraph/codeintellify.createHoverifier}, which uses it to fetch
 * the list of buttons to display on the hover tooltip. This function in turn determines that by looking at all
 * action contributions for "hover". It also defines two builtin hover actions, for "Go to definition" and "Find
 * references".
 */
export function getHoverActions(
    { extensionsController }: ExtensionsControllerProps,
    context: HoveredToken & HoverContext
): Observable<ActionItemProps[]> {
    const params: TextDocumentPositionParams = {
        textDocument: { uri: makeRepoURI(context) },
        position: { line: context.line - 1, character: context.character - 1 },
    }
    const definitionURLOrError = getDefinitionURL(extensionsController.services.textDocumentDefinition, params).pipe(
        map(result => (result ? result.url : result)), // we only care about the URL or null, not whether there are multiple
        catchError(err => [asError(err) as ErrorLike]),
        share()
    )

    return combineLatest(
        // To reduce UI jitter, don't show "Go to definition" until (1) the result or an error was received or (2)
        // the fairly long LOADER_DELAY has elapsed.
        merge(
            [undefined], // don't block on the first emission
            of(LOADING).pipe(
                delay(LOADER_DELAY),
                takeUntil(definitionURLOrError)
            ),
            definitionURLOrError
        ),

        // Only show "Find references" if a reference provider is registered. Unlike definitions, references are
        // not preloaded and here just involve statically constructing a URL, so no need to indicate loading.
        extensionsController.services.textDocumentReferences
            .providersForDocument(params.textDocument)
            .pipe(map(providers => providers.length !== 0)),

        // If there is no definition, delay showing "Find references" because it is likely that the token is
        // punctuation or something else that has no meaningful references. This reduces UI jitter when it can be
        // quickly determined that there is no definition. TODO(sqs): Allow reference providers to register
        // "trigger characters" or have a "hasReferences" method to opt-out of being called for certain tokens.
        merge(
            of(true).pipe(
                delay(LOADER_DELAY),
                takeUntil(definitionURLOrError.pipe(filter(v => !!v)))
            ),
            definitionURLOrError.pipe(
                filter(v => !!v),
                map(v => !!v)
            )
        ).pipe(startWith(false))
    ).pipe(
        switchMap(([definitionURLOrError, hasReferenceProvider, showFindReferences]) =>
            extensionsController.services.contribution
                .getContributions(undefined, {
                    'goToDefinition.loading': definitionURLOrError === LOADING,
                    'goToDefinition.url':
                        (definitionURLOrError !== LOADING &&
                            !isErrorLike(definitionURLOrError) &&
                            definitionURLOrError) ||
                        null,
                    'goToDefinition.notFound':
                        definitionURLOrError !== LOADING &&
                        !isErrorLike(definitionURLOrError) &&
                        definitionURLOrError === null,
                    'goToDefinition.error': isErrorLike(definitionURLOrError),

                    'findReferences.url':
                        hasReferenceProvider && showFindReferences
                            ? toPrettyBlobURL({ ...context, position: context, viewState: 'references' })
                            : null,

                    // Store hoverPosition for the goToDefinition action's commandArguments to refer to.
                    hoverPosition: params as any,
                })
                .pipe(map(contributions => getContributedActionItems(contributions, ContributableMenu.Hover)))
        )
    )
}

/**
 * Returns an observable that emits null if no definitions are found, {url, multiple:false} if exactly 1 definition
 * is found, {url: defPanelURL, multiple:true} if multiple definitions are found, or an error.
 */
export function getDefinitionURL(
    textDocumentDefinition: Pick<Services['textDocumentDefinition'], 'getLocations'>,
    params: TextDocumentPositionParams
): Observable<{ url: string; multiple: boolean } | null> {
    return textDocumentDefinition.getLocations(params).pipe(
        map(definitions => {
            if (definitions === null || (Array.isArray(definitions) && definitions.length === 0)) {
                return null
            }

            if (Array.isArray(definitions) && definitions.length > 1) {
                // Open the panel to show all definitions.
                const ctx = parseRepoURI(params.textDocument.uri) as AbsoluteRepoFilePosition
                return {
                    url: toPrettyBlobURL({
                        ...ctx,
                        position: { line: params.position.line + 1, character: params.position.character + 1 },
                        viewState: 'def',
                    }),
                    multiple: true,
                }
            }

            const def = Array.isArray(definitions) ? definitions[0] : definitions

            // TODO!(sqs): this only works for web, not for client/browser -- the final URL for defs is different
            // because client/browser tries to keep you on the site (eg stay on GitHub after go-to-def). Need to
            // factor out the "generate URL to blob position" logic.

            const uri = parseRepoURI(def.uri)
            if (def.range) {
                uri.position = {
                    line: def.range.start.line + 1,
                    character: def.range.start.character + 1,
                }
            }
            return {
                url: toPrettyBlobURL({
                    ...uri,
                    rev: uri.rev || uri.commitID || '',
                    filePath: uri.filePath || '',
                }),
                multiple: false,
            }
        })
    )
}

/**
 * Registers contributions for hover-related functionality.
 */
export function registerHoverContributions({
    extensionsController,
    history,
}: ExtensionsControllerProps & { history: H.History }): Unsubscribable {
    const subscriptions = new Subscription()

    // Registers the "Go to definition" action shown in the hover tooltip. When clicked, the action finds the
    // definition of the token using the registered definition providers and navigates the user there.
    //
    // When the user hovers over a token (even before they click "Go to definition"), it attempts to preload the
    // definition. If preloading succeeds and at least 1 definition is found, the "Go to definition" action becomes
    // a normal link (<a href>) pointing to the definition's URL. Using a normal link here is good for a11y and UX
    // (e.g., open-in-new-tab works and the browser status bar shows the URL).
    //
    // Otherwise (if preloading fails, or if preloading has not yet finished), clicking "Go to definition" executes
    // the goToDefinition command. A loading indicator is displayed, and any errors that occur during execution are
    // shown to the user.
    //
    // Future improvements:
    //
    // TODO(sqs): If the user middle-clicked or Cmd/Ctrl-clicked the button, it would be nice if when the
    // definition was found, a new browser tab was opened to the destination. This is not easy because browsers
    // usually block new tabs opened by JavaScript not directly triggered by a user mouse/keyboard interaction.
    //
    // TODO(sqs): Pin hover after an action has been clicked and before it has completed.
    subscriptions.add(
        extensionsController.services.contribution.registerContributions({
            contributions: {
                actions: [
                    {
                        id: 'goToDefinition',
                        title: 'Go to definition',
                        command: 'goToDefinition',
                        commandArguments: [
                            // tslint:disable:no-invalid-template-strings
                            '${json(hoverPosition)}',
                            // tslint:enable:no-invalid-template-strings
                        ],
                    },
                    {
                        // This action is used when preloading the definition succeeded and at least 1
                        // definition was found.
                        id: 'goToDefinition.preloaded',
                        title: 'Go to definition',
                        command: 'open',
                        // tslint:disable-next-line:no-invalid-template-strings
                        commandArguments: ['${goToDefinition.url}'],
                    },
                ],
                menus: {
                    hover: [
                        // Do not show any actions if no definition provider is registered. (In that case,
                        // goToDefinition.{error, loading, url} will all be falsey.)
                        {
                            action: 'goToDefinition',
                            when: 'goToDefinition.error || goToDefinition.loading',
                        },
                        {
                            action: 'goToDefinition.preloaded',
                            when: 'goToDefinition.url',
                        },
                    ],
                },
            },
        })
    )
    subscriptions.add(
        extensionsController.services.commands.registerCommand({
            command: 'goToDefinition',
            run: async (paramsStr: string) => {
                const params: TextDocumentPositionParams = JSON.parse(paramsStr)
                const result = await getDefinitionURL(extensionsController.services.textDocumentDefinition, params)
                    .pipe(first())
                    .toPromise()
                if (!result) {
                    throw new Error('No definition found.')
                }
                if (result.url === H.createPath(history.location)) {
                    // The user might be confused if they click "Go to definition" and don't go anywhere, which
                    // occurs if they are *already* on the definition. Give a helpful tip if they do this.
                    //
                    // Note that these tips won't show up if the definition URL is already known by the time they
                    // click "Go to definition", because then it's a normal link and not a button that executes
                    // this command. TODO: It would be nice if they also showed up in that case.
                    if (result.multiple) {
                        // The user may not have noticed the panel at the bottom of the screen, so tell them
                        // explicitly.
                        throw new Error('Multiple definitions shown in panel below.')
                    }
                    throw new Error('Already at the definition.')
                }
                history.push(result.url)
            },
        })
    )

    // Register the "Find references" action shown in the hover tooltip. This is simpler than "Go to definition"
    // because it just needs a URL that can be statically constructed from the current URL (it does not need to
    // query any providers).
    subscriptions.add(
        extensionsController.services.contribution.registerContributions({
            contributions: {
                actions: [
                    {
                        id: 'findReferences',
                        title: 'Find references',
                        command: 'open',
                        // tslint:disable-next-line:no-invalid-template-strings
                        commandArguments: ['${findReferences.url}'],
                    },
                ],
                menus: {
                    hover: [
                        // To reduce UI jitter, even though "Find references" can be shown immediately (because
                        // the URL can be statically constructed), don't show it until either (1) "Go to
                        // definition" is showing or (2) the LOADER_DELAY has elapsed. The part (2) of this
                        // logic is implemented in the observable pipe that sets findReferences.url above.
                        {
                            action: 'findReferences',
                            when:
                                'findReferences.url && (goToDefinition.loading || goToDefinition.url || goToDefinition.error || goToDefinition.notFound)',
                        },
                    ],
                },
            },
        })
    )

    return subscriptions
}
