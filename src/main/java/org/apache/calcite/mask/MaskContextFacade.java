package org.apache.calcite.mask;

import com.google.common.collect.Maps;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class MaskContextFacade {
    private static final ConcurrentMap<String, MaskContext> RUNNING_CTX_MAP = Maps.newConcurrentMap();
    private static final ThreadLocal<MaskContext> CURRENT_CTX = ThreadLocal.withInitial(() -> {
        MaskContext context = new MaskContext();
        RUNNING_CTX_MAP.put(context.getMaskId(), context);
        return context;
    });

    public static MaskContext current() {
        return CURRENT_CTX.get();
    }

    public static void resetCurrent() {
        MaskContext maskContext = CURRENT_CTX.get();
        if (maskContext != null) {
            RUNNING_CTX_MAP.remove(maskContext.getMaskId());
            CURRENT_CTX.remove();
        }
    }

    public static MaskContext getCurrentContext(String maskId){
        return RUNNING_CTX_MAP.get(maskId);
    }
}
