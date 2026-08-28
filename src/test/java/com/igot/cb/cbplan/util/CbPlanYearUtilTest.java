package com.igot.cb.cbplan.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.Month;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbPlanYearUtilTest {

    @ParameterizedTest
    @CsvSource({
            "2026, 8, 12, 2026-27",
            "2026, 2, 12, 2025-26",
            "2026, 4, 1, 2026-27",
            "2026, 3, 31, 2025-26",
            "2026, 12, 31, 2026-27",
            "2026, 1, 1, 2025-26"
    })
    void testResolveFinancialYearForDate(int year, int month, int day, String expected) {
        assertEquals(expected, CbPlanYearUtil.resolveFinancialYearForDate(LocalDate.of(year, month, day)));
    }

    @Test
    void testResolveFinancialYearAcrossCenturyBoundary() {
        assertEquals("2099-00", CbPlanYearUtil.resolveFinancialYearForDate(
                LocalDate.of(2099, Month.APRIL, 1)));
    }

    @Test
    void testResolveCurrentFinancialYearMatchesExpectedFormat() {
        String currentFy = CbPlanYearUtil.resolveCurrentFinancialYear();
        assertTrue(currentFy.matches("^\\d{4}-\\d{2}$"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"2026-27", "2025-26", "1999-00"})
    void testIsValidPlanYearFormatAcceptsValidYears(String planYear) {
        assertTrue(CbPlanYearUtil.isValidPlanYearFormat(planYear));
    }

    @ParameterizedTest
    @ValueSource(strings = {"2026-30", "2026/27", "202-27", "2026-2027", "abcd-ef", "   ", ""})
    void testIsValidPlanYearFormatRejectsInvalidYears(String planYear) {
        assertFalse(CbPlanYearUtil.isValidPlanYearFormat(planYear));
    }

    @Test
    void testIsValidPlanYearFormatRejectsNull() {
        assertFalse(CbPlanYearUtil.isValidPlanYearFormat(null));
    }

    @Test
    void testValidateAndNormalizeTrimsValidYear() {
        assertEquals("2026-27", CbPlanYearUtil.validateAndNormalize("  2026-27  "));
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "2026-30", "  "})
    void testValidateAndNormalizeReturnsNullForInvalid(String planYear) {
        assertNull(CbPlanYearUtil.validateAndNormalize(planYear));
    }

    @Test
    void testValidateAndNormalizeReturnsNullForNull() {
        assertNull(CbPlanYearUtil.validateAndNormalize(null));
    }
}
