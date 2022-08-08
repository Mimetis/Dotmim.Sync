import { UseQueryResult, useQuery } from 'react-query';
import { Scope } from '../models';


export const useScopes = (): UseQueryResult<Scope[], Error> => {

  const callApiAsync = async () => {

    // preparing the headers
    const headers = new Headers();
    headers.append('Content-Type', 'application/json');
    headers.append('Accept', 'application/json');

    var requestInit: RequestInit = {
      method: 'GET',
      headers: headers,
    };

    var response = await fetch('/api/scopes', requestInit);

    if (!response) throw new Error(`No response available for /api/scopes`);
    else if (response.status < 200 || response.status > 204) {
      var message = await response.text();
      throw new Error(message);
    }
    return await response.json();
  };

  // calling API
  const queryResult = useQuery<Scope[], Error>(['scopes'], callApiAsync, {
    refetchInterval: 0,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  return queryResult;
}