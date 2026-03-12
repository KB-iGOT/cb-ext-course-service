package com.igot.cb.storage.service;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class StorageServiceImpl implements StorageService {

	private final Logger logger = LoggerFactory.getLogger(getClass().getName());
	private BaseStorageService storageService = null;

	@Autowired
	private CbExtServerProperties serverProperties;

	@PostConstruct
	public void init() {
		if (storageService == null) {
			storageService = StorageServiceFactory.getStorageService(new StorageConfig(
					serverProperties.getCloudStorageTypeName(), serverProperties.getCloudStorageKey(),
					serverProperties.getCloudStorageSecret().replace("\\n", "\n"), Option.apply(serverProperties.getCloudStorageEndpoint()), Option.empty()));
		}
	}

	@Override
	public ApiResponse uploadFile(MultipartFile mFile, String cloudFolderName) throws IOException {
		return uploadFile(mFile, cloudFolderName, serverProperties.getCloudContainerName());
	}

	@Override
	public ApiResponse uploadFile(File file, String cloudFolderName, String containerName) {
		ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FILE_UPLOAD);
		try {
			String objectKey = cloudFolderName + "/" + file.getName();
			String url = storageService.upload(containerName, file.getAbsolutePath(),
					objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty());
			Map<String, String> uploadedFile = new HashMap<>();
			uploadedFile.put(Constants.NAME, file.getName());
			uploadedFile.put(Constants.URL, url);
			response.getResult().putAll(uploadedFile);
			return response;
		} catch (Exception e) {
			logger.error("Failed to upload file. Exception: ", e);
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			return response;
		} finally {
			if (file != null) {
				file.delete();
			}
		}
	}

	protected void finalize() {
		try {
			if (storageService != null) {
				storageService.closeContext();
				storageService = null;
			}
		} catch (Exception e) {
		}
	}
	@Override
	public ApiResponse downloadFile(String fileName, String containerName) {
		ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FILE_DOWNLOAD);
		try {
			String objectKey = containerName + "/" + fileName;
			storageService.download(serverProperties.getCloudContainerName(), objectKey, Constants.LOCAL_BASE_PATH,
					Option.apply(Boolean.FALSE));
			return response;
		} catch (Exception e) {
			logger.error("Failed to download the file: " + fileName + ", Exception: ", e);
			response.getParams().setStatus(Constants.FAILED);
			response.getParams().setErrmsg("Failed to download the file. Exception: " + e.getMessage());
			response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
			return response;
		}
	}
}
