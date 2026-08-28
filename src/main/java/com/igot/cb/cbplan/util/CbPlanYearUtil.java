package com.igot.cb.cbplan.util;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.regex.Pattern;

/**
 * Utility class for CB Plan year operations (financial year resolution and validation).
 *
 * @version 3.0
 */
@Slf4j
public final class CbPlanYearUtil {
    private static final Pattern PLAN_YEAR_PATTERN = Pattern.compile("^\\d{4}-\\d{2}$");
    private static final Month FY_START_MONTH = Month.APRIL;

    private CbPlanYearUtil() {
    }

    /**
     * Resolves the current financial year based on Indian FY cycle (April 1 - March 31).
     * Examples:
     * - Date: 2026-08-12 → Returns "2026-27"
     * - Date: 2026-02-12 → Returns "2025-26"
     * - Date: 2026-04-01 → Returns "2026-27"
     * - Date: 2026-03-31 → Returns "2025-26"
     *
     * @return current financial year in format "YYYY-YY"
     */
    public static String resolveCurrentFinancialYear() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Kolkata"));
        return resolveFinancialYearForDate(today);
    }

    /**
     * Resolves the financial year for a specific date.
     *
     * @param date the date to resolve FY for
     * @return financial year in format "YYYY-YY"
     */
    public static String resolveFinancialYearForDate(LocalDate date) {
        int year = date.getYear();
        Month month = date.getMonth();
        if (month.getValue() >= FY_START_MONTH.getValue()) {
            int nextYear = year + 1;
            return String.format("%d-%02d", year, nextYear % 100);
        } else {
            int prevYear = year - 1;
            return String.format("%d-%02d", prevYear, year % 100);
        }
    }

    /**
     * Validates plan year format (YYYY-YY).
     * Expected format: 4-digit year, hyphen, 2-digit year suffix.
     * Examples: "2026-27", "2025-26"
     *
     * @param planYear the plan year string to validate
     * @return true if valid format, false otherwise
     */
    public static boolean isValidPlanYearFormat(String planYear) {
        if (planYear == null || planYear.isBlank()) {
            return false;
        }
        if (!PLAN_YEAR_PATTERN.matcher(planYear.trim()).matches()) {
            return false;
        }
        try {
            String[] parts = planYear.trim().split("-");
            int startYear = Integer.parseInt(parts[0]);
            int endYearSuffix = Integer.parseInt(parts[1]);
            int expectedEndYearSuffix = (startYear + 1) % 100;
            if (endYearSuffix != expectedEndYearSuffix) {
                log.warn("Invalid plan year: {} - end year suffix should be {}", planYear, expectedEndYearSuffix);
                return false;
            }
            return true;
        } catch (NumberFormatException e) {
            log.error("Failed to parse plan year: {}", planYear, e);
            return false;
        }
    }

    /**
     * Validates and normalizes plan year string.
     * Returns trimmed plan year if valid, null otherwise.
     *
     * @param planYear the plan year to validate
     * @return trimmed plan year if valid, null otherwise
     */
    public static String validateAndNormalize(String planYear) {
        if (planYear == null || planYear.isBlank()) {
            return null;
        }
        String normalized = planYear.trim();
        return isValidPlanYearFormat(normalized) ? normalized : null;
    }
}
