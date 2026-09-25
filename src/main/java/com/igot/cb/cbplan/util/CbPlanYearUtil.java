package com.igot.cb.cbplan.util;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.regex.Pattern;

/**
 * Utility class for CB Plan year operations and validation.
 *
 * @version 3.0
 */
@Slf4j
public final class CbPlanYearUtil {
    private static final Pattern PLAN_YEAR_PATTERN = Pattern.compile("^\\d{4}-\\d{2}$");
    private CbPlanYearUtil() {
    }

    /**
     * Resolves the current plan year from the calendar year in Asia/Kolkata.
     * Examples:
     * - Date: 2026-08-12 → Returns "2026-27"
     * - Date: 2026-02-12 → Returns "2026-27"
     *
     * @return current plan year in format "YYYY-YY"
     */
    public static String resolveCurrentFinancialYear() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Kolkata"));
        return resolveFinancialYearForDate(today);
    }

    /**
     * Resolves the plan year for a specific date using its calendar year.
     *
     * @param date the date to resolve FY for
     * @return plan year in format "YYYY-YY"
     */
    public static String resolveFinancialYearForDate(LocalDate date) {
        int year = date.getYear();
        return String.format("%d-%02d", year, (year + 1) % 100);
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

    /**
     * Resolves the previous plan year for the given plan year.
     * Example: "2026-27" → "2025-26"
     *
     * @param planYear the plan year in format "YYYY-YY"
     * @return previous plan year in format "YYYY-YY"
     */
    public static String resolvePreviousYear(String planYear) {
        int startYear = Integer.parseInt(planYear.substring(0, 4)) - 1;
        return String.format("%d-%02d", startYear, (startYear + 1) % 100);
    }
}
