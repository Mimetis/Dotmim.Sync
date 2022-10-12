import { UseQueryResult, useQuery, QueryKey } from 'react-query';
import { SyncLog } from '../models';


export const useSyncLogs = (clientScopeId?: string): UseQueryResult<SyncLog[], Error> => {

  const callApiAsync = async () => {

    // preparing the headers
    const headers = new Headers();
    headers.append('Content-Type', 'application/json');
    headers.append('Accept', 'application/json');

    const requestInit: RequestInit = {
      method: 'GET',
      headers: headers,
    };

    let response:Response;
    if (clientScopeId) {
      response = await fetch(`/api/synclogs/${clientScopeId}`, requestInit);
    } else {

      response = await fetch('/api/SyncLogs', requestInit);
    }

    if (!response) throw new Error(`No response available for /api/SyncLogs`);
    else if (response.status < 200 || response.status > 204) {
      const message = await response.text();
      throw new Error(message);
    }

    return await response.json();
  };

  const queryKey:QueryKey = clientScopeId ?['synclogs', clientScopeId] :  ['synclogs'];

  // calling API
  const queryResult = useQuery<SyncLog[], Error>(queryKey, callApiAsync, {
    refetchInterval: 0,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  return queryResult;
}