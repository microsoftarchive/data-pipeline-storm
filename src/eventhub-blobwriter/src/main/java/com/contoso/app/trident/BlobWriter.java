// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.BlockSearchMode;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.core.Base64;

@SuppressWarnings("unused")
public class BlobWriter {
	static public void upload(Properties properties, String blobname, String blockIdStr, String data) {
		Logger logger = (Logger) LoggerFactory.getLogger(BlobWriter.class);
		InputStream stream = null;
		try {
			if (LogSetting.LOG_BLOB_WRITER && LogSetting.LOG_METHOD_BEGIN) {
				logger.info("upload Begin");
			}

			String accountName = properties.getProperty("storage.blob.account.name");
			String accountKey = properties.getProperty("storage.blob.account.key");
			String containerName = properties.getProperty("storage.blob.account.container");
			String connectionStrFormatter = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s";
			String connectionStr = String.format(connectionStrFormatter, accountName, accountKey);

			if (LogSetting.LOG_BLOB_WRITER) {
				logger.info("upload accountName = " + accountName);
				logger.info("upload accountKey = " + accountKey);
				logger.info("upload containerName = " + containerName);
				logger.info("upload connectionStr = " + connectionStr);
				logger.info("upload blobname = " + blobname);
				logger.info("upload blockIdStr = " + blockIdStr);
			}
			if (LogSetting.LOG_BLOB_WRITER_DATA) {
				logger.info("upload data= \r\n" + data);
			}
			CloudStorageAccount account = CloudStorageAccount.parse(String.format(connectionStr, accountName, accountKey));
			CloudBlobClient _blobClient = account.createCloudBlobClient();
			CloudBlobContainer _container = _blobClient.getContainerReference(containerName);
			_container.createIfNotExists();
			CloudBlockBlob blockBlob = _container.getBlockBlobReference(blobname);
			BlobRequestOptions blobOptions = new BlobRequestOptions();
			stream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
			BlockEntry newBlock = new BlockEntry(Base64.encode(blockIdStr.getBytes()), BlockSearchMode.UNCOMMITTED);

			ArrayList<BlockEntry> blocksBeforeUpload = new ArrayList<BlockEntry>();
			if (blockBlob.exists(AccessCondition.generateEmptyCondition(), blobOptions, null)) {
				blocksBeforeUpload = blockBlob.downloadBlockList(BlockListingFilter.COMMITTED, null, blobOptions, null);
			}
			if (LogSetting.LOG_BLOB_WRITER_BLOCKLIST_BEFORE_UPLOAD) {
				int i = 0;
				String id = null;
				for (BlockEntry e : blocksBeforeUpload) {
					i++;
					id = e.getId();
					logger.info("BlockEntry Before Upload id=" + id + ", Index = " + i);
				}
				if (id != null) {
					logger.info("BlockEntry Before Upload id=" + id + ", Index = " + i + " --last before");
				}
			}

			blockBlob.uploadBlock(newBlock.getId(), stream, -1);
			if (!blocksBeforeUpload.contains(newBlock)) {
				blocksBeforeUpload.add(newBlock);
			}

			if (LogSetting.LOG_BLOB_WRITER_BLOCKLIST_AFTER_UPLOAD) {
				int i = 0;
				String id = null;
				for (BlockEntry e : blocksBeforeUpload) {
					i++;
					id = e.getId();
					logger.info("BlockEntry After Upload id=" + id + ", Index = " + i);
				}
				if (id != null) {
					logger.info("BlockEntry After Upload id=" + id + ", Index = " + i + " --last after");
				}
			}

			blockBlob.commitBlockList(blocksBeforeUpload);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FailedException(e.getMessage());
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		if (LogSetting.LOG_BLOB_WRITER && LogSetting.LOG_METHOD_END) {
			logger.info("upload End");
		}
	}

	static public void remove(Properties properties, String blockIdStrFormat, String blobname, String blockIdStr) {
		// remove blocks with blockid >= blockIdStr
		Logger logger = (Logger) LoggerFactory.getLogger(BlobWriter.class);
		try {
			if (LogSetting.LOG_BLOB_WRITER && LogSetting.LOG_METHOD_BEGIN) {
				logger.info("remove Begin");
			}

			String accountName = properties.getProperty("storage.blob.account.name");
			String accountKey = properties.getProperty("storage.blob.account.key");
			String containerName = properties.getProperty("storage.blob.account.container");

			String connectionStrFormatter = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s";
			String connectionStr = String.format(connectionStrFormatter, accountName, accountKey);

			logger.info("accountName = " + accountName);
			logger.info("accountKey = " + accountKey);
			logger.info("containerName = " + containerName);
			logger.info("connectionStr = " + connectionStr);

			CloudStorageAccount account = CloudStorageAccount.parse(String.format(connectionStr, accountName, accountKey));
			CloudBlobClient _blobClient = account.createCloudBlobClient();
			CloudBlobContainer _container = _blobClient.getContainerReference(containerName);
			_container.createIfNotExists();
			CloudBlockBlob blockBlob = _container.getBlockBlobReference(blobname);
			BlobRequestOptions blobOptions = new BlobRequestOptions();

			ArrayList<BlockEntry> blocksBeforeUpload = new ArrayList<BlockEntry>();
			if (blockBlob.exists(AccessCondition.generateEmptyCondition(), blobOptions, null)) {
				blocksBeforeUpload = blockBlob.downloadBlockList(BlockListingFilter.COMMITTED, null, blobOptions, null);
			}
			int blockid = Integer.parseInt(blockIdStr);
			int size = blocksBeforeUpload.size();
			// int size = 50000;
			for (int i = size; i >= blockid; i--) {
				String idStr = String.format(blockIdStrFormat, i);
				BlockEntry entry = new BlockEntry(Base64.encode(idStr.getBytes()), BlockSearchMode.UNCOMMITTED);
				if (blocksBeforeUpload.contains(entry)) {
					blocksBeforeUpload.remove(entry);
				}
			}
			blockBlob.commitBlockList(blocksBeforeUpload);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FailedException(e.getMessage());
		}
		if (LogSetting.LOG_BLOB_WRITER && LogSetting.LOG_METHOD_END) {
			logger.info("remove End");
		}

	}
}
