import assert from 'assert'
import { of } from 'rxjs'
import { first } from 'rxjs/operators'
import { TextDocumentPositionParams } from '../api/protocol'
import { Location } from '../api/protocol/plainTypes'
import { getDefinitionURL } from './actions'

const FIXTURE_PARAMS: TextDocumentPositionParams = {
    textDocument: { uri: 'git://r?v#f' },
    position: { line: 1, character: 1 },
}

const FIXTURE_LOCATION: Location = {
    uri: 'git://r2?v2#f2',
    range: {
        start: { line: 2, character: 2 },
        end: { line: 3, character: 3 },
    },
}

describe.only('getHoverActions', () => {
    it('asdf', () => {
        assert.deepStrictEqual('a', 'a')
    })
})

describe.only('getDefinitionURL', () => {
    it('emits null if the locations result is null', async () => {
        assert.strictEqual(
            await getDefinitionURL(
                {
                    getLocation: () => of(null),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            null
        )
    })

    it('emits null if the locations result is empty', async () => {
        assert.strictEqual(
            await getDefinitionURL(
                {
                    getLocation: () => of([]),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            null
        )
    })

    it('emits the definition URL if there is exactly 1 location result', async () => {
        assert.strictEqual(
            await getDefinitionURL(
                {
                    getLocation: () => of<Location[]>([FIXTURE_LOCATION]),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            '/r2@v2/-/blob/f2#L3:3'
        )
    })

    it('emits the definition panel URL if there is more than 1 location result', async () => {
        assert.strictEqual(
            await getDefinitionURL(
                {
                    getLocation: () => of<Location[]>([FIXTURE_LOCATION, { ...FIXTURE_LOCATION, uri: 'other' }]),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            '/r@v/-/blob/f#L2:2&tab=def'
        )
    })
})
