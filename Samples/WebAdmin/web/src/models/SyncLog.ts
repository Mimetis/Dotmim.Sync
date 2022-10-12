export type SyncLog = {
    sessionId: string;
    clientScopeId: string;
    scopeName: string;
    scopeParameters?: string;
    state: string;
    error: string;
    syncType: string;
    startTime: Date;
    endTime: Date;
    isNew: boolean;
    fromTimestamp: number;
    toTimestamp: number;
    changesAppliedOnServer?: string;
    changesAppliedOnClient?: string;
    snapshotChangesAppliedOnClient?: string;
    clientChangesSelected?: string;
    serverChangesSelected?: string;
    network:string;

    details?: SyncLogDetail[];
};

export type SyncLogDetail = {
    sessionId: string;
    clientScopeId: string;
    tableName: string;
    scopeName: string;
    scopeParameters?: string;
    state: string;
    command: string;
    totalChangesSelected: string;
    tableChangesUpsertsApplied: string;
    tableChangesDeletesApplied: string;
};

export type Scope = {
    name: string;
    setup: any;
    lastCleanup: any;
    version: string;
};

export type ClientScope = {
    id: string;
    scopeName: string;
    lastSync: any;
    lastSyncDuration: any;
    lastSyncTimestamp: string;
    properties: string;
};
