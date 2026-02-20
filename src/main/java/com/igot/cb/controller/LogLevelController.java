package com.igot.cb.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/logs")
@RequiredArgsConstructor
@Slf4j
public class LogLevelController {

    private final LoggingSystem loggingSystem;

    @PostMapping("/level")
    public String changeLogLevel(@RequestParam(required = false) String level) {

        if (null == level  || level.trim().isEmpty()) {
            return "Log level must not be empty. Use DEBUG, INFO, WARN, ERROR";
        }

        LogLevel logLevel;
        try {
            logLevel = LogLevel.valueOf(level.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return "Invalid log level... Use DEBUG, INFO, WARN, ERROR";
        }

        // ROOT = whole application
        loggingSystem.setLogLevel("ROOT", logLevel);

        return "Log level changed to : " + logLevel;
    }

}