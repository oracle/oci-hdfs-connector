package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.hdfs.BmcConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

/**
 * This test is designed to simulate the process of a user authenticating using Kerberos,
 * then using a service principal to generate a SPNEGO token for that user.
 */
public class SpnegoGeneratorTest {
    private MiniKdc miniKdc;
    private File kdcDir;
    private Configuration configuration;
    private TokenGenerator tokenGenerator;

    @Before
    public void setUp() throws Exception {
        Properties kdcConf = MiniKdc.createConf();
        kdcDir = new File(System.getProperty("test.dir", "target"), "minikdc");
        miniKdc = new MiniKdc(kdcConf, kdcDir);
        miniKdc.start();

        File userKeytab = new File(kdcDir, "user.keytab");
        File serviceKeytab = new File(kdcDir, "service.keytab");
        miniKdc.createPrincipal(userKeytab, "user/localhost");
        miniKdc.createPrincipal(serviceKeytab, "HTTP/localhost");

        System.setProperty("java.security.krb5.conf", miniKdc.getKrb5conf().getAbsolutePath());
        System.setProperty("sun.security.krb5.debug", "true");

        configuration = new Configuration();
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        configuration.set(BmcConstants.TOKEN_EXCHANGE_SERVICE_PRINCIPAL_KEY, "HTTP/localhost");
        configuration.set("hadoop.security.authentication.kerberos.keytab", serviceKeytab.getAbsolutePath());

        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("user/localhost", userKeytab.getAbsolutePath());

        tokenGenerator = new SpnegoGenerator(configuration);
    }

    /**
     * Test the generation of a SPNEGO token using the authenticated user's context.
     */
    @Test
    public void testGenerateSpnegoToken() throws Exception {
        String spnegoToken = tokenGenerator.generateToken();
        assertNotNull(spnegoToken);
    }

    @Test(expected = RuntimeException.class)
    public void testGenerateTokenFailure() throws Exception {
        UserGroupInformation mockedUGI = Mockito.mock(UserGroupInformation.class);
        Mockito.when(mockedUGI.getLoginUser()).thenThrow(new IOException("Mocked Kerberos failure"));

        UserGroupInformation.setLoginUser(mockedUGI);

        tokenGenerator.generateToken();
    }

    @After
    public void tearDown() throws Exception {
        if (miniKdc != null) {
            miniKdc.stop();
        }
    }
}
