package org.eclipse.ecsp.analytics.stream.base.utils;

import org.eclipse.ecsp.stream.dma.dao.DeviceStatusUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import java.util.Map;
import java.util.Optional;

/**
 * class DeviceStatusUtilTest.
 */
public class DeviceStatusUtilTest {

    @InjectMocks
    private DeviceStatusUtil deviceStatusUtil;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testgetMapParentKey() {
        String mapPatenrKey =
                deviceStatusUtil.getMapParentKey("testService", Optional.of("subService"));
        Assert.assertNotNull(mapPatenrKey);
    }

    @Test
    public void testgetsubServiceToParentKeyMapping() {
        deviceStatusUtil.validateServiceName("test");
        Map<String, String> mapPatenrKey = deviceStatusUtil.getSubServiceToParentKeyMapping();
        Assert.assertNotNull(mapPatenrKey);
    }

}
