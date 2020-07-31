package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.Exceptions;
import org.springframework.http.HttpHeaders;

/** Generic interface class for implementing different authentication mechanisms.
 *
 * @author Jonathan Essex
 */
public interface LoginHandler {

    /** Apply credentials to a request.
     * 
     * Function will perform a login if necessary and apply the resulting credentials to the given request.
     *
     *
     * @param mainRequest The request that requires authentication information.
     * @throws com.softwareplumbers.dms.Exceptions.ServerError if login cannot be performed.
     */
    void applyCredentials(HttpHeaders mainRequest) throws Exceptions.ServerError;

    /** Get credentials
     *
     * @return Credentials cookie as a string.
     */
    String getCredentials();
    
}
