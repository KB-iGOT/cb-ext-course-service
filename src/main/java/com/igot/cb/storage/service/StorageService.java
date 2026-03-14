package com.igot.cb.storage.service;

import com.igot.cb.model.ApiResponse;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

public interface StorageService {
	ApiResponse uploadFile(MultipartFile file, String containerName) throws IOException;

	ApiResponse uploadFile(File file, String cloudFolderName, String containerName);

	ApiResponse downloadFile(String fileName, String containerName);
}
