package com.softwareplumbers.dms.rest.client.spring;

import java.net.HttpCookie;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Optional;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import com.softwareplumbers.keymanager.KeyManager;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import org.springframework.beans.factory.annotation.Required;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Handle login to the doctane proxy.
 * 
 * @author SWPNET\jonessex
 *
 */
public class SignedRequestLoginHandler implements LoginHandler {

    //------ private static variables -------//
    
    private static final Logger LOG = Logger.getLogger(SignedRequestLoginHandler.class.getName());

    private static String BASE_COOKIE_NAME="DoctaneUserToken";

    //------ private variables -------//

    private HttpCookie authCookie;
    private KeyManager<SecretKeys,KeyPairs> keyManager;
    private UriTemplate authURI;
    private String cookieName;

    //------ private static methods -------//
    
    private static byte[] formatAuthRequest(KeyPairs serviceAccount) {
        JsonObjectBuilder authRequest = Json.createObjectBuilder();
        authRequest.add("instant", System.currentTimeMillis());
        authRequest.add("account", serviceAccount.name());
        return authRequest.build().toString().getBytes();
    }
    
    private static Optional<HttpCookie> getCookieFromResponse(String cookieName, ResponseEntity<?> response) {
        return response.getHeaders().get("Set-Cookie").stream()
            .map(HttpCookie::parse)
            .flatMap(List::stream)
            .filter(cookie -> cookieName.equals(cookie.getName()))
            .findAny();
    }

    //------ private methods ------///
    
    private byte[] signAuthRequest(byte[] request, KeyPairs serviceAccount) {
        LOG.log(Level.FINER, ()->"entering signAuthRequest with " + request + "," + serviceAccount);
        KeyPair pair = keyManager.getKeyPair(serviceAccount);
        Signature sig;
        try {
            sig = Signature.getInstance("SHA1withDSA", "SUN");
            sig.initSign(pair.getPrivate());
            sig.update(request);
            byte[] result = sig.sign();
            LOG.log(Level.FINER, "signAuthRequest returns <redacted>");
            return result;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException e) {
            LOG.log(Level.FINER, "signAuthRequest rethrowing ", e);
            throw new IllegalArgumentException("cannot find an appropriate key pair for " + serviceAccount);
        } catch (SignatureException e) {
            LOG.log(Level.FINER, "signAuthRequest rethrowing ", e);
            throw new RuntimeException(e);
        }
    }
        
    private Optional<HttpCookie> getCookieFromServer() {
        LOG.log(Level.FINER, "entering getCookieFromServer");
        RestTemplate restTemplate = new RestTemplate();
        byte[] authRequestBytes = formatAuthRequest(KeyPairs.DEFAULT_SERVICE_ACCOUNT);
        byte[] signature = signAuthRequest(authRequestBytes, KeyPairs.DEFAULT_SERVICE_ACCOUNT);
        Encoder base64 = Base64.getUrlEncoder();
        String authRequestBase64 = base64.encodeToString(authRequestBytes);
        String sigBase64 = base64.encodeToString(signature);
        URI authRequest = authURI.expand(authRequestBase64, sigBase64); 
        ResponseEntity<String> response = restTemplate.exchange(authRequest, HttpMethod.GET, null, String.class);
        Optional<HttpCookie> result = getCookieFromResponse(cookieName, response);
        LOG.log(Level.FINER, "getCookieFromServer returns {0}", result);
        return result;
    }
    
    //------- public methods ------//
    
    /** Create a new LoginHandler
     * 
     * @param keyManager A key manager that contains a key for DEFAULT_SERVICE_ACCOUNT
     * @param authURI The URI for the authorization service
     * @param repository The repository which we are accessing
     */
    public SignedRequestLoginHandler(KeyManager<SecretKeys,KeyPairs> keyManager, UriTemplate authURI, String repository) {
        LOG.log(Level.FINER, ()->"entering constructor with <KeyManager>, " + authURI + "," + repository);
        this.keyManager = keyManager;
        this.authURI = authURI;
        this.cookieName = "DoctaneUserToken/"+repository;
        LOG.log(Level.FINER, "exiting constructor");
    }
    
    public SignedRequestLoginHandler() {
        LOG.log(Level.FINER, "entering no-arg constructor");
        this.keyManager = null;
        this.authURI = null;
        this.cookieName = null;
        LOG.log(Level.FINER, "exiting constructor");
    }
    
    @Required
    public void setKeyManager(KeyManager<SecretKeys, KeyPairs> keyManager) { 
        LOG.log(Level.FINER, "setting keyManager to {0}", keyManager);
        this.keyManager = keyManager;
    }

    @Required
    public void setAuthURI(String authURI) { 
        LOG.log(Level.FINER, "setting authURI to {0}", authURI);
        this.authURI = new UriTemplate(authURI);
    }
    
    @Required
    public void setRepository(String repository) { 
        LOG.log(Level.FINER, "setting repository to {0}", repository);
        this.cookieName = "DoctaneUserToken/"+repository;
    }
    
    /** Apply credentials to a request.
     * 
     * Function will perform a login if necessary and apply the resulting credentials to the given request.
     * 
     * @param mainRequest The request that requires authentication information.
     */
    @Override
    public void applyCredentials(HttpHeaders mainRequest) {
        LOG.log(Level.FINER, "entering applyCredentials");
        if (authCookie == null || authCookie.hasExpired()) {
            Optional<HttpCookie> cookie = getCookieFromServer();
            if (cookie.isPresent()) authCookie = cookie.get();
        }
        if (authCookie != null)
            mainRequest.add("Cookie", authCookie.toString());
        LOG.log(Level.FINER, "exiting applyCredentials");
    }
    
    /** Get credentials 
     * 
     * @return Credentials cookie as a string.
     */
    @Override
    public String getCredentials() {
        LOG.log(Level.FINER, "entering getCredentials");
        if (authCookie == null || authCookie.hasExpired()) {
            Optional<HttpCookie> cookie = getCookieFromServer();
            if (cookie.isPresent()) authCookie = cookie.get();
        }
        return authCookie.toString();
    }
}
