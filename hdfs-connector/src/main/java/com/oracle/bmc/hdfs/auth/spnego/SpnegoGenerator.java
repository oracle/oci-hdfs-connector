package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import com.sun.security.auth.module.Krb5LoginModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * This class provides the functionality to generate SPNEGO tokens for Kerberos authentication.
 * It implements the {@link TokenGenerator} interface and allows generating tokens
 * using either internal or external `kinit` based on the configuration provided.
 */
public class SpnegoGenerator implements TokenGenerator {
    private static final Logger log = LoggerFactory.getLogger(SpnegoGenerator.class);
    private final BmcPropertyAccessor propertyAccessor;

    public SpnegoGenerator(Configuration configuration) {
        this.propertyAccessor = new BmcPropertyAccessor(configuration, null);
    }

    @Override
    public String generateToken() throws IOException, InterruptedException {
        Boolean useInternalKinit = propertyAccessor.asBoolean().get(BmcProperties.ENABLE_INTERNAL_KINIT_FOR_TOKEN_EXCHANGE);
        if (useInternalKinit) {
            log.debug("Using internal kinit for token generation.");
            return generateTokenWithInternalKinit();
        } else {
            log.debug("Using external kinit for token generation.");
            return generateTokenWithExternalKinit();
        }
    }

    private GSSContext initializeGSSContext() throws GSSException {
        Oid krb5Mechanism = KerberosUtil.GSS_KRB5_MECH_OID;
        Oid krb5PrincipalNameType = KerberosUtil.NT_GSS_KRB5_PRINCIPAL_OID;
        Oid spnegoOid = KerberosUtil.GSS_SPNEGO_MECH_OID;

        GSSManager manager = GSSManager.getInstance();
        GSSName gssServerName = manager.createName(
                propertyAccessor.asString().get(BmcProperties.TOKEN_EXCHANGE_SERVICE_PRINCIPAL),
                krb5PrincipalNameType,
                krb5Mechanism
        );
        GSSContext gssContext = manager.createContext(gssServerName, spnegoOid, null, GSSContext.DEFAULT_LIFETIME);
        gssContext.requestCredDeleg(true);
        gssContext.requestMutualAuth(true);
        gssContext.requestLifetime(3600);
        return gssContext;
    }

    /**
     * Generates a SPNEGO token using an external `kinit` process with Kerberos client
     * for Kerberos authentication.
     */
    public String generateTokenWithExternalKinit() throws IOException, InterruptedException {
        byte[] spnegoToken = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<byte[]>() {
            @Override
            public byte[] run() throws Exception {
                GSSContext gssContext = null;
                byte[] token = null;
                try {
                    gssContext = initializeGSSContext();
                    token = gssContext.initSecContext(new byte[0], 0, 0);
                } catch (GSSException e) {
                    log.error("Failed to generate the SPNEGO token due to a GSS exception: " + e.getMessage(), e);
                    throw new RuntimeException("GSSException encountered in generateTokenWithExternalKinit", e);
                } finally {
                    disposeGSSContext(gssContext);
                }
                return token;
            }
        });

        if (spnegoToken == null) {
            log.error("Failed to generate the SPNEGO token.");
            throw new RuntimeException("Failed to generate the SPNEGO token. Token is null.");
        }
        return Base64.getEncoder().encodeToString(spnegoToken);
    }

    /**
     * Generates a SPNEGO token using the internal `Krb5LoginModule` with Kerberos client
     * for Kerberos authentication.
     */
    public String generateTokenWithInternalKinit() {
        Subject subject = getAuthenticateSubject();
        return Subject.doAs(subject, (PrivilegedAction<String>) () -> {
            GSSContext gssContext = null;
            byte[] token = null;
            try {
                gssContext = initializeGSSContext();
                token = gssContext.initSecContext(new byte[0], 0, 0);
            } catch (GSSException e) {
                log.error("Failed to generate the SPNEGO token due to a GSS exception: " + e.getMessage(), e);
                throw new RuntimeException("GSSException encountered in generateTokenWithInternalKinit", e);
            } finally {
                disposeGSSContext(gssContext);
            }

            if (token == null) {
                log.error("Failed to generate the SPNEGO token.");
                throw new RuntimeException("Failed to generate the SPNEGO token. Token is null.");
            }
            return Base64.getEncoder().encodeToString(token);
        });
    }

    private void disposeGSSContext(GSSContext gssContext) {
        if (gssContext != null) {
            try {
                gssContext.dispose();
            } catch (GSSException e) {
                log.warn("Error disposing GSSContext: {}", e.getMessage(), e);
            }
        }
    }

    public Subject getAuthenticateSubject() {
        final Map<String, String> options = new HashMap<>();
        options.put("keyTab", propertyAccessor.asString().get(BmcProperties.IAM_TOKEN_EXCHANGE_KEYTAB_PATH));
        options.put("principal", propertyAccessor.asString().get(BmcProperties.TOKEN_EXCHANGE_USER_PRINCIPAL));
        options.put("doNotPrompt", "true");
        options.put("isInitiator", "true");
        options.put("refreshKrb5Config", "true");
        options.put("storeKey", "true");
        options.put("useKeyTab", "true");

        Subject subject = new Subject();
        Krb5LoginModule krb5LoginModule = new Krb5LoginModule();
        krb5LoginModule.initialize(subject, null, new HashMap<String, String>(), options);
        try {
            krb5LoginModule.login();
            krb5LoginModule.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return subject;
    }
}
