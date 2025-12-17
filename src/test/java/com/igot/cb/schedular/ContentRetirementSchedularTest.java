package com.igot.cb.schedular;

import com.igot.cb.service.ContentRetirementService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ContentRetirementSchedularTest {

    @Mock
    private ContentRetirementService contentRetirementService;

    private ContentRetirementSchedular contentRetirementSchedular;

    @BeforeEach
    void setUp() {
        contentRetirementSchedular = new ContentRetirementSchedular(contentRetirementService);
    }

    @Test
    void runDaily_ShouldCallProcessDueRetirements() {
        contentRetirementSchedular.runDaily();

        verify(contentRetirementService).processDueRetirements();
    }
}