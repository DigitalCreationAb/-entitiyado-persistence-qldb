import { Persistence } from "@digitalcreation/aws-lambda-actors/src/persistence";
import { Event } from "@digitalcreation/aws-lambda-actors/src/event"
import {QldbDriver, RetryConfig, TransactionExecutor} from "amazon-qldb-driver-nodejs";
import { ClientConfiguration } from "aws-sdk/clients/acm";
import { Agent } from "https";
import { dom } from "ion-js";

export class QLDBPersistence implements Persistence {
    private _driver: QldbDriver;

    constructor(region: string, ledgerName: string, maxConcurrentTransactions: number = 10) {
        const agentForQldb: Agent = new Agent({
            keepAlive: true,
            maxSockets: maxConcurrentTransactions
        });

        const serviceConfigurationOptions: ClientConfiguration = {
            region: region,
            httpOptions: {
                agent: agentForQldb
            }
        };

        const retryConfig: RetryConfig = new RetryConfig(4);

        this._driver = new QldbDriver(
            ledgerName,
            serviceConfigurationOptions,
            maxConcurrentTransactions,
            retryConfig);
    }

    loadEvents(entityId: string): Promise<Event[]> {
        return this._driver.executeLambda(async (txn: TransactionExecutor) => {
            const resultList: dom.Value[] = (await txn.execute('SELECT entityId, events FROM Commits WHERE entityId = ?', entityId)).getResultList();

            return resultList
                .flatMap(x => JSON.parse(x.get('events')?.stringValue() || '[]') as Event[]);
        });
    }

    async storeEvents(entityId: string, events: Event[]): Promise<void> {
        const commitData: Record<string, any> = {
            entityId: entityId,
            events: JSON.stringify(events)
        };

        await this._driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute('INSERT INTO Commits ?', commitData);
        });

        return Promise.resolve(undefined);
    }
}
