import { ActionDefinition, DetailedView, ESStack, Executor, QueryDefinition, RepositoryContext, ViewDefinition, ViewEventBuilderHandler } from "@sctrl/event-stack/types";

export function createPlatformEventsExecutor(baseUrl: string): Executor {
    async function compileView<T = null>(stack: ESStack, view: ViewDefinition<T>, context?: RepositoryContext): Promise<T> {
        const stackName = view.esDefinition.type;
        const viewName = view.type;

        const url = `${baseUrl}/v1/views/${encodeURIComponent(stackName)}/${encodeURIComponent(viewName)}/${encodeURIComponent(stack.namespace)}`;

        console.log("compileView", url);
        const response = await fetch(url)
            .then(res => { if (res.ok) return res; throw new Error(`Error`); })
            .then(res => res.json())
            .then(res => res.data);

        return response;
    }

    async function compileQuery<T = null, U = null>(stack: ESStack, query: QueryDefinition<T, U>, parameters: U, maxEventId?: number, context?: RepositoryContext): Promise<DetailedView<T>> {
        const stackName = query.esDefinition.type;
        const queryName = query.type;

        const url = `${baseUrl}/v1/query/${encodeURIComponent(stackName)}/${encodeURIComponent(queryName)}/${encodeURIComponent(stack.namespace)}`;

        console.log("compileQuery", url);
        return await fetch(url, {
            method: "POST",
            body: JSON.stringify(parameters),
        })
            .then(res => { if (res.ok) return res; throw new Error(`Error`); })
            .then(res => res.json());
    }

    async function executeAction(stack: ESStack, action: ActionDefinition, actionPayload: any): Promise<void> {
        const stackName = action.esDefinition.type;
        const actionName = action.type;

        const url = `${baseUrl}/v1/actions/${encodeURIComponent(stackName)}/${encodeURIComponent(actionName)}/${encodeURIComponent(stack.namespace)}`;

        console.log("executeAction", url);
        await fetch(url, {
            method: "POST",
            body: JSON.stringify(actionPayload),
        })
            .then(res => { if (res.ok) return res; throw new Error(`Error`); });
    }

    return {
        compileView,
        compileQuery,
        executeAction,
    };
}