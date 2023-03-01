import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { ddbStore, ddbViewCache } from "./storage/ddb";
import { EventBridge } from "@aws-sdk/client-eventbridge";

import { compileQuery, compileView, executeAction } from "@sctrl/event-stack/lib";
import { ActionDefinition, EventStackDefinition, QueryDefinition, ViewDefinition } from "@sctrl/event-stack/types";
import { convertToAlphaNumberic } from "./utils";

export interface CreateEventPlatformOptions {
    eventBusName: string;
    eventsTableName: string;
    viewsTableName: string;
}

export function createEventPlatform(options: CreateEventPlatformOptions) {
    const eventBus = new aws.cloudwatch.EventBus(`event-bus`, {
        name: options.eventBusName,
    });

    const eventBusLogGroup = new aws.cloudwatch.LogGroup(`event-bus-log-group`, {
        name: `/aws/events/sctrl/event-bus/${options.eventBusName}`,
    });

    const role = new aws.iam.Role(`event-bus-role`, {
        assumeRolePolicy: JSON.stringify({
            Version: "2012-10-17",
            Statement: [
                {
                    Effect: "Allow",
                    Principal: {
                        Service: "events.amazonaws.com",
                    },
                    Action: "sts:AssumeRole",
                },
            ],
        }),
        inlinePolicies: [{
            name: "event-bus-policy",
            policy: JSON.stringify({
                Version: "2012-10-17",
                Statement: [
                    {
                        Effect: "Allow",
                        Action: [
                            "*",
                        ],
                        Resource: "*",
                    },
                ],
            })
        }],
    });

    // Log all events to CloudWatch
    const eventBusRule = new aws.cloudwatch.EventRule(`event-bus-rule`, {
        name: `${options.eventBusName}-rule`,
        isEnabled: true,
        eventBusName: eventBus.name,
        eventPattern: JSON.stringify({
            source: [{ prefix: "sctrl." }],
        }),
        // roleArn: role.arn,
    });

    const eventBusTarget = new aws.cloudwatch.EventTarget(`event-bus-target`, {
        rule: eventBusRule.name,
        arn: eventBusLogGroup.arn,
        eventBusName: eventBus.name,

    });



    const restApi = new aws.apigatewayv2.Api("api_gateway", {
        protocolType: "HTTP",
        routeSelectionExpression: "$request.method $request.path",
        corsConfiguration: {
            allowOrigins: ["*"],
            allowMethods: ["*"],
            allowHeaders: ["*"],
        },
    });

    const apiStage = new aws.apigatewayv2.Stage(`api_gateway_stage`, {
        apiId: restApi.id,
        autoDeploy: true,
        name: "$default",
    });

    const dynamoEventsTable = new aws.dynamodb.Table("ddb_events_table", {
        name: options.eventsTableName,
        attributes: [
            { name: "namespace", type: "S" },
            { name: "id", type: "N" },
        ],
        billingMode: "PAY_PER_REQUEST",
        hashKey: "namespace",
        rangeKey: "id",
        serverSideEncryption: {
            enabled: true,
        },
    });

    const dynamoViewsTable = new aws.dynamodb.Table("ddb_views_table", {
        name: options.viewsTableName,
        attributes: [
            { name: "namespace", type: "S" },
            { name: "viewName", type: "S" },
        ],
        billingMode: "PAY_PER_REQUEST",
        hashKey: "namespace",
        rangeKey: "viewName",
        serverSideEncryption: {
            enabled: true,
        },
    });

    return {
        eventBusName: options.eventBusName,
        eventsTableName: options.eventsTableName,
        viewsTableName: options.viewsTableName,
        restApiId: restApi.id,
        restApiExecutionArn: restApi.executionArn,
    } as CreateEventStackConfig;
}

export interface CreateEventStackConfig {
    eventBusName: string;
    eventsTableName: string;
    viewsTableName: string;
    restApiId: pulumi.Output<string>;
    restApiExecutionArn: pulumi.Output<string>;
}

export function setupEventStack(eventStack: EventStackDefinition, cfg: CreateEventStackConfig) {
    createActionsInAWS(eventStack, cfg);

    for (const view of Object.values(eventStack.views)) {
        createViewProjectorInAWS(eventStack.type, view, cfg);
        createViewGetterInAWS(eventStack.type, view, cfg);
    }

    for (const query of Object.values(eventStack.queries)) {
        createQueryInAWS(eventStack.type, query, cfg);
    }
}

function createActionsInAWS(eventStack: EventStackDefinition, cfg: CreateEventStackConfig) {
    Object.values(eventStack.actions).forEach((_action: any) => {
        const action = _action as ActionDefinition;

        const lambda = new aws.lambda.CallbackFunction(`${eventStack.type}_actions_${action.type}`, {
            callback: async (event: any) => {
                const eventbridge = new EventBridge({ region: "us-west-2" });
                const itemId = decodeURIComponent(event.pathParameters.id);
                console.log("test");

                const store = ddbStore(cfg.eventsTableName);
                const stack = await store.getOrCreateStack(eventStack.type, itemId);
                const matchingAction = eventStack.actions[action.type as any];
                try {
                    await executeAction(stack, matchingAction, JSON.parse(event.body));
                    await eventbridge.putEvents({
                        Entries: [
                            {
                                EventBusName: `${cfg.eventBusName}`,
                                Source: `sctrl.events`,
                                DetailType: `${eventStack.type}.${action.type}`,
                                Detail: JSON.stringify({
                                    id: itemId,
                                }),
                            },
                        ],
                    });

                    return {
                        statusCode: 204,
                        headers: {
                            "Content-Type": "application/json",
                        },
                        body: "",
                    };
                } catch (ex) {
                    if ((ex as any).action) {
                        return {
                            statusCode: 409,
                            headers: {
                                "Content-Type": "application/json",
                            },
                            body: JSON.stringify({
                                message: (ex as any).errorMessage,
                                action: (ex as any).action,
                                result: (ex as any).result,
                            }),
                        };
                    }

                    console.error(ex);

                    return {
                        statusCode: 500,
                        headers: {
                            "Content-Type": "application/json",
                        },
                        body: JSON.stringify({
                            message: `Internal server error`,
                        }),
                    };
                }

            },
            runtime: "nodejs18.x",
        });

        const path = `/v1/actions/${eventStack.type}/${action.type}/{id}`;

        createApiGatewayRoute(cfg, lambda, path, "POST");
    });
}

function createViewProjectorInAWS(prefix: string, view: ViewDefinition, cfg: CreateEventStackConfig) {
    const eventNames = Object.entries(view.events).map(([eventName, eventDefinition]) => `${prefix}.${eventName}`);

    if (!eventNames.length) return;

    const lambda = new aws.lambda.CallbackFunction(`${prefix}_view_${view.type}`, {
        callback: async (event: any) => {
            const store = ddbStore(cfg.eventsTableName);
            const cache = ddbViewCache({
                namespace: prefix,
                tablename: cfg.viewsTableName,
            });
            const stack = await store.getOrCreateStack(prefix, event.detail.id);

            await compileView(stack, view, {
                viewCache: cache,
            });

            return {};
        },
        runtime: "nodejs18.x",
    });

    const rule = new aws.cloudwatch.EventRule(`${prefix}_${view.type}_rule`, {
        eventPattern: JSON.stringify({
            source: [`sctrl.events`],
            "detail-type": eventNames,
        }),
        isEnabled: true,
        eventBusName: cfg.eventBusName,
        name: `${prefix}_${view.type}_rule`,
    });

    const target = new aws.cloudwatch.EventTarget(`${prefix}_${view.type}_target`, {
        rule: rule.name,
        eventBusName: cfg.eventBusName,
        arn: lambda.arn,
    });


    // Ensure lambda function is invokable by event bus, create a trigger
    const permission = new aws.lambda.Permission(`${prefix}_view_${view.type}`, {
        action: "lambda:InvokeFunction",
        function: lambda,
        principal: "events.amazonaws.com",
        sourceArn: pulumi.interpolate`arn:aws:events:${aws.config.region}:770331712051:rule/${cfg.eventBusName}/${prefix}_${view.type}_rule`,
    });
}

function createQueryInAWS(prefix: string, query: QueryDefinition, cfg: CreateEventStackConfig) {
    const lambda = new aws.lambda.CallbackFunction(`${prefix}_query_${query.type}`, {
        callback: async (event: any) => {
            const itemId = decodeURIComponent(event.pathParameters.id);

            const store = ddbStore(cfg.eventsTableName);
            const cache = ddbViewCache({
                namespace: prefix,
                tablename: cfg.viewsTableName,
            });
            const stack = await store.getOrCreateStack(prefix, itemId);

            const output = await compileQuery(stack, query, JSON.parse(event.body), undefined, {
                viewCache: undefined,
            });

            return {
                statusCode: 200,
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    data: output,
                }),
            };
        },
        runtime: "nodejs18.x",
    });

    const path = `/v1/query/${prefix}/${query.type}/{id}`;

    // Register lambda function as API endpoint
    createApiGatewayRoute(cfg, lambda, path, "POST");
}

function createViewGetterInAWS(prefix: string, view: ViewDefinition, cfg: CreateEventStackConfig) {
    const lambda = new aws.lambda.CallbackFunction(`${prefix}_view_getter_${view.type}`, {
        callback: async (event: any) => {
            const itemId = decodeURIComponent(event.pathParameters.id);

            const store = ddbStore(cfg.eventsTableName);
            const cache = ddbViewCache({
                namespace: prefix,
                tablename: cfg.viewsTableName,
            });
            const stack = await store.getOrCreateStack(prefix, itemId);

            const output = await compileView(stack, view, {
                viewCache: cache,
            });

            return {
                statusCode: 200,
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    data: output,
                }),
            };
        },
        runtime: "nodejs18.x",
    });

    const path = `/v1/views/${prefix}/${view.type}/{id}`;

    createApiGatewayRoute(cfg, lambda, path, "GET");
}

function createApiGatewayRoute(cfg: CreateEventStackConfig, lambda: aws.lambda.CallbackFunction<any, { statusCode: number; headers: { "Content-Type": string; }; body: string; }>, path: string, method: string) {
    const prefix = convertToAlphaNumberic(`${method}_${path}`);

    const integration = new aws.apigatewayv2.Integration(`${prefix}_integration`, {
        apiId: cfg.restApiId,
        integrationType: "AWS_PROXY",
        integrationUri: lambda.arn,
        payloadFormatVersion: "2.0",
    });

    const route = new aws.apigatewayv2.Route(`${prefix}_integration`, {
        apiId: cfg.restApiId,
        routeKey: `${method} ${path}`,
        target: pulumi.interpolate`integrations/${integration.id}`,
    });


    // Ensure lambda function is invoked when API endpoint is called
    const permission = new aws.lambda.Permission(`${prefix}_integration`, {
        action: "lambda:InvokeFunction",
        function: lambda,
        principal: "apigateway.amazonaws.com",
        sourceArn: pulumi.interpolate`${cfg.restApiExecutionArn}/*/*${path}`,
    });
}

export interface ESAction {
    stackType: string;
    action: string;
}

export function actionSelector<T extends string | null>(es: EventStackDefinition<T>, action: T) {
    return {
        stackType: es.type,
        action,
    };
}

export function actionTrigger(name: string, system: CreateEventStackConfig, esAction: ESAction, target: aws.lambda.CallbackFunction<any, any>) {
    const rule = new aws.cloudwatch.EventRule(`${esAction.stackType}_${esAction.action}_${name}_rule`, {
        eventPattern: JSON.stringify({
            source: [`sctrl.events`],
            "detail-type": [`${esAction.stackType}.${esAction.action}`],
        }),
        isEnabled: true,
        eventBusName: system.eventBusName,
        name: `${esAction.stackType}_${esAction.action}_${name}_rule`,
    });

    const cloudTarget = new aws.cloudwatch.EventTarget(`${esAction.stackType}_${esAction.action}_${name}_target`, {
        rule: rule.name,
        eventBusName: system.eventBusName,
        arn: target.arn,
    });

    // Ensure lambda function is invokable by event bus, create a trigger
    const permission = new aws.lambda.Permission(`${esAction.stackType}_${esAction.action}_${name}_permission`, {
        action: "lambda:InvokeFunction",
        function: target,
        principal: "events.amazonaws.com",
        sourceArn: pulumi.interpolate`arn:aws:events:${aws.config.region}:770331712051:rule/${system.eventBusName}/${esAction.stackType}_${esAction.action}_${name}_rule`,
    });
}
