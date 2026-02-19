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
    public String changeLogLevel(@RequestParam String level) {

        LogLevel logLevel;
        try {
            logLevel = LogLevel.valueOf(level.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return "Invalid log level... Use DEBUG, INFO, WARN, ERROR";
        }

        // ROOT = whole application
        loggingSystem.setLogLevel("ROOT", logLevel);

        return "Log level changed to : " + logLevel;
    }

}

