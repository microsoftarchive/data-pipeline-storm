// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

public final class LogSetting {

	// todo: move those constants to Config.properties

	public static final boolean LOG_MESSAGE = false;

	public static final boolean LOG_BATCH = true;
	public static final boolean LOG_INSTANCE = true;

	public static final boolean LOG_BLOCK_ROLL_OVER = false;

	public static final boolean LOG_BLOCK = false;
	public static final boolean LOG_PERSIST = true;
	public static final boolean LOG_GET_LAST_BLOCK = true;
	public static final boolean LOG_GET_FIRST_BLOCK = true;

	public static final boolean LOG_BLOB_WRITER = false;
	public static final boolean LOG_BLOB_WRITER_DATA = false;
	public static final boolean LOG_BLOB_WRITER_BLOCKLIST_BEFORE_UPLOAD = false;
	public static final boolean LOG_BLOB_WRITER_BLOCKLIST_AFTER_UPLOAD = false;

	public static final boolean LOG_REDIS = true;

	public static final boolean LOG_METHOD_BEGIN = false;
	public static final boolean LOG_METHOD_END = false;
}
