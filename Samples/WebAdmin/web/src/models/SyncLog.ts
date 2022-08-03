export type SyncLog =
  {

    sessionId: string;
    clientScopeId: string;
    scopeName: string;
    syncType: string;
    startTime: Date;
    isNew: boolean;
    fromTimestamp: number;
    toTimestamp: number;
    totalChangesSelected: number;
    totalChangesSelectedUpdates: number;
    totalChangesSelectedDeletes: number;
    totalChangesApplied: number;
    totalChangesAppliedUpdates: number;
    totalChangesAppliedDeletes: number;
    totalResolvedConflicts: number;
    details?: SyncLogDetail[];
  }

export type SyncLogDetail = {
  sessionId: string;
  clientScopeId: string;
  tableName: string;
  scopeName: string;
  command: string;
  totalChangesSelected: number;
  totalChangesSelectedUpdates: number;
  totalChangesSelectedDeletes: number;
  totalChangesApplied: number;
  totalChangesAppliedUpdates: number;
  totalChangesAppliedDeletes: number;
  totalResolvedConflicts: number;
}

export type Scope = {
  name: string;
  setup: any;
  lastsync: any;
  version: string;
}

export type ClientScope = {
  id: string;
  scopeName: string;
  lastSync: any;
  lastSyncDuration: any;
  lastSyncTimestamp: string;
  properties: string
}