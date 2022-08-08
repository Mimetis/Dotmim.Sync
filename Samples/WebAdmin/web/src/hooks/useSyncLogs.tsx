import { UseQueryResult, useQuery } from 'react-query';
import { SyncLog } from '../models';


export const useSyncLogs = (): UseQueryResult<SyncLog[], Error> => {

  const callApiAsync = async () => {

    // preparing the headers
    const headers = new Headers();
    headers.append('Content-Type', 'application/json');
    headers.append('Accept', 'application/json');

    var requestInit: RequestInit = {
      method: 'GET',
      headers: headers,
    };

    var response = await fetch('/api/SyncLogs', requestInit);

    if (!response) throw new Error(`No response available for /api/SyncLogs`);
    else if (response.status < 200 || response.status > 204) {
      var message = await response.text();
      throw new Error(message);
    }

    return await response.json();
  };

  // calling API
  const queryResult = useQuery<SyncLog[], Error>(['SyncLogs'], callApiAsync, {
    refetchInterval: 0,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  return queryResult;
}