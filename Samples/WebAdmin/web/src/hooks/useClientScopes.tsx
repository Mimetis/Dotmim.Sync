import { UseQueryResult, useQuery } from 'react-query';
import { ClientScope } from '../models/SyncLog';


export const useClientScopes = (): UseQueryResult<ClientScope[], Error> => {

  const callApiAsync = async () => {

    // preparing the headers
    const headers = new Headers();
    headers.append('Content-Type', 'application/json');
    headers.append('Accept', 'application/json');

    const requestInit: RequestInit = {
      method: 'GET',
      headers: headers,
    };

    const response = await fetch('/api/clientsScopes', requestInit);

    if (!response) throw new Error(`No response available for /api/clientsScopes`);
    else if (response.status < 200 || response.status > 204) {
      const message = await response.text();
      throw new Error(message);
    }
    return await response.json();
  };

  // calling API
  const queryResult = useQuery<ClientScope[], Error>(['clientsScopes'], callApiAsync, {
    refetchInterval: 0,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  return queryResult;
}