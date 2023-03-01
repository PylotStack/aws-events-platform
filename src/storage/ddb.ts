import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { compileView } from "@sctrl/event-stack/lib";
import { CompiledView, ESEvent, ESStack, LocalStore, ViewCache } from "@sctrl/event-stack/types";

export interface DDBStackOptions {
    ddb: DynamoDBDocument;
    tablename: string;
    namespace: string;
}

export interface DDBViewCacheOptions {
    ddb?: DynamoDBDocument;
    tablename: string;
    namespace: string;
}

let sharedDBB: DynamoDBDocument;

function createDDBStack(options: DDBStackOptions): ESStack {
    async function commitEvent(ev: ESEvent) {
        if (ev.id < 0) return;

        const event = {
            ...ev,
            namespace: options.namespace,
        };

        const transactItems: any[] = [
            {
                Put: {
                    TableName: options.tablename,
                    Item: event,
                    ConditionExpression: "attribute_not_exists(#ns)",
                    ExpressionAttributeNames: {
                        "#ns": "namespace",
                    },
                },
            }
        ];

        if (event.id !== 0) {
            transactItems.push({
                ConditionCheck: {
                    TableName: options.tablename,
                    Key: {
                        namespace: event.namespace,
                        id: event.id - 1
                    },
                    ConditionExpression: "attribute_exists(#ns)",
                    ExpressionAttributeNames: {
                        "#ns": "namespace",
                    },
                },
            });
        }

        try {
            await options.ddb.transactWrite({
                TransactItems: transactItems,
            });
        } catch (ex) {
            console.log(`Tried committing event with id: ${event.id}`, transactItems);
            if (~(ex as any)?.message?.indexOf("[ConditionalCheckFailed")) {
                console.log(`An event with id: ${event.id} already exists`);
            }

            if (~(ex as any)?.message?.indexOf("ConditionalCheckFailed]")) {
                console.log(`Event too far ahead. An event with id: ${event.id - 1} doesn't exist yet so an event with id: ${event.id} cannot be created.`);
            }

            throw ex;
        }
    }

    async function commitAnonymousEvent(ev: ESEvent) {
        const res = await options.ddb.query({
            TableName: options.tablename,
            KeyConditionExpression: "#ns=:ns",
            ScanIndexForward: false,
            ExpressionAttributeNames: {
                "#ns": "namespace",
            },
            ExpressionAttributeValues: {
                ":ns": options.namespace,
            },
            Limit: 1,
        });

        const lastEvent = res?.Items?.[0];
        const nextEventId = (lastEvent?.id ?? -1) + 1;

        return await commitEvent({
            ...ev,
            id: nextEventId,
        });
    }

    async function getEvent(id: number) {
        const event = await options.ddb.get({
            TableName: options.tablename,
            Key: {
                namespace: options.namespace,
                id: id,
            },
        });

        return event.Item as ESEvent;
    }

    async function slice(start?: number, end?: number) {
        console.log("slice", {
            TableName: options.tablename,
            KeyConditionExpression: "#ns=:ns and #id >= :start",
            ExpressionAttributeNames: {
                "#ns": "namespace",
                "#id": "id",
            },
            ExpressionAttributeValues: {
                ":ns": options.namespace,
                ":start": start,
            },
        });
        const res = await options.ddb.query({
            TableName: options.tablename,
            KeyConditionExpression: "#ns=:ns and #id >= :start",
            ExpressionAttributeNames: {
                "#ns": "namespace",
                "#id": "id",
            },
            ExpressionAttributeValues: {
                ":ns": options.namespace,
                ":start": start,
            },
        });

        const items: ESEvent[] = res.Items as ESEvent[];
        return items;
    }

    return {
        namespace: options.namespace,
        commitAnonymousEvent,
        commitEvent,
        getEvent,
        slice,
    };
}

export function ddbStore(tablename: string): LocalStore {
    const stacks = new Map<string, ESStack>();

    function name(stackSet: string, stack: string) {
        return `${stackSet}|${stack}`;
    }

    async function getStack(stackSet: string, stackName: string): Promise<ESStack> {
        return stacks.get(name(stackSet, stackName)) as ESStack;
    }

    async function createStack(stackSet: string, stackName: string): Promise<ESStack> {
        const _name = name(stackSet, stackName);
        sharedDBB = sharedDBB ?? DynamoDBDocument.from(new DynamoDBClient({ region: "us-west-2" }));
        const stack = createDDBStack({
            ddb: sharedDBB,
            namespace: _name,
            tablename,
        });
        stacks.set(_name, stack);
        return stack;
    }

    async function getOrCreateStack(stackSet: string, stackName: string): Promise<ESStack> {
        const existingStack = await getStack(stackSet, stackName);
        return existingStack
            ? existingStack
            : await createStack(stackSet, stackName);
    }

    return {
        getStack,
        createStack,
        getOrCreateStack,
    };
}

export function ddbViewCache(options: DDBViewCacheOptions): ViewCache {
    options.ddb = options.ddb ?? sharedDBB;


    async function getFromCache(identifier: string): Promise<CompiledView> {
        const res = await options.ddb?.get({
            TableName: options.tablename,
            Key: {
                namespace: options.namespace,
                viewName: identifier,
            }
        });

        return res?.Item as CompiledView;
    }

    async function updateCache(identifier: string, compiledView: CompiledView) {
        try {
            await options.ddb?.put({
                TableName: options.tablename,
                Item: {
                    namespace: options.namespace,
                    viewName: identifier,
                    ...compiledView,
                },
                ConditionExpression: "attribute_not_exists(#ns) OR #evid < :evid",
                ExpressionAttributeNames: {
                    "#ns": "namespace",
                    "#evid": "eventId",
                },
                ExpressionAttributeValues: {
                    ":evid": compiledView.eventId,
                }
            });
        } catch (ex) {
            if (~(ex as any)?.message?.indexOf("ConditionalCheckFailed")) return;
            throw ex;
        }
    }

    return {
        getFromCache,
        updateCache,
    };
}
