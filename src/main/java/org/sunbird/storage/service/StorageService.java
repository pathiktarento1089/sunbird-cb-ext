package org.sunbird.storage.service;

import java.io.File;
import java.io.IOException;

import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

public interface StorageService {
	public SBApiResponse uploadFile(MultipartFile file, String containerName) throws IOException;

	SBApiResponse uploadFile(File mFile, String containerName);

	public SBApiResponse deleteFile(String fileName, String containerName);
	SBApiResponse downloadFile(String fileName);
}
