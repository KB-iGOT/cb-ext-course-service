package com.igot.cb.service;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TestUtils {
    public static void setField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokePrivate(Object target, String methodName, Class<?> paramType, Object arg) {
        try {
            Method method = target.getClass().getDeclaredMethod(methodName, paramType);
            method.setAccessible(true);
            return (T) method.invoke(target, arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
