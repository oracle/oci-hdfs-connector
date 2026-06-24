package com.oracle.bmc.hdfs.auth.spnego;


import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

public class UPSTManagerFactoryTest {

    @Test
    public void testCreateUPSTManager() throws Exception {
        Configuration conf = new Configuration();
        UPSTManagerFactory factory = new UPSTManagerFactory(conf);

        UPSTManager manager = factory.createUPSTManager();
        assertNotNull(manager);

        Field tokenGeneratorField = UPSTManager.class.getDeclaredField("tokenGenerator");
        tokenGeneratorField.setAccessible(true);
        Object tokenGenerator = tokenGeneratorField.get(manager);
        assertTrue(tokenGenerator instanceof SpnegoGenerator);

        Field rsaKeyPairField = UPSTManager.class.getDeclaredField("rsaKeyPairGenerator");
        rsaKeyPairField.setAccessible(true);
        Object rsaKeyGenerator = rsaKeyPairField.get(manager);
        assertTrue(rsaKeyGenerator instanceof RSAKeyPairGenerator);

        Field tokenExchangeField = UPSTManager.class.getDeclaredField("tokenExchangeClient");
        tokenExchangeField.setAccessible(true);
        Object tokenExchangeClient = tokenExchangeField.get(manager);
        assertTrue(tokenExchangeClient instanceof IAMTokenExchangeClient);

        Field accessorField = SpnegoGenerator.class.getDeclaredField("propertyAccessor");
        accessorField.setAccessible(true);
        BmcPropertyAccessor spnegoAccessor = (BmcPropertyAccessor) accessorField.get(tokenGenerator);

        Field confField = BmcPropertyAccessor.class.getDeclaredField("configuration");
        confField.setAccessible(true);
        Object spnegoConf = confField.get(spnegoAccessor);
        assertSame(conf, spnegoConf);

        Field iamAccessorField = IAMTokenExchangeClient.class.getDeclaredField("propertyAccessor");
        iamAccessorField.setAccessible(true);
        BmcPropertyAccessor iamAccessor = (BmcPropertyAccessor) iamAccessorField.get(tokenExchangeClient);
        Object iamConf = confField.get(iamAccessor);
        assertSame(conf, iamConf);
    }
}
