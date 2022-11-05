/*
 * Copyright (C) 2011 readyState Software Ltd, 2007 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.readystatesoftware.sqliteasset;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A helper class to manage database creation and version management using an application's raw
 * asset files.
 *
 * <p>This class provides developers with a simple way to ship their Android app with an existing
 * SQLite database (which may be pre-populated with data) and to manage its initial creation and any
 * upgrades required with subsequent version releases.
 *
 * <p>This class makes it easy for {@link android.content.ContentProvider} implementations to defer
 * opening and upgrading the database until first use, to avoid blocking application startup with
 * long-running database upgrades.
 *
 * <p>For examples see <a href="https://github.com/jgilfelt/android-sqlite-asset-helper">
 * https://github.com/jgilfelt/android-sqlite-asset-helper</a>
 *
 * <p>
 *
 * <p class="note"><strong>Note:</strong> this class assumes monotonically increasing version
 * numbers for upgrades. Also, there is no concept of a database downgrade; installing a new version
 * of your app which uses a lower version number than a previously-installed version will result in
 * undefined behavior.
 */
public class SQLiteAssetHelper extends SQLiteOpenHelper {

  private static final String ASSET_DB_PATH = "databases";

  private final Context context;
  private final String databaseName;
  private final CursorFactory factory;
  private final int newVersion;

  private SQLiteDatabase database = null;
  private boolean isInitializing = false;

  private final String databasePath;

  private final String assetPath;

  private final String upgradePathFormat;

  private int forcedUpgradeVersion = 0;

  /**
   * Create a helper object to create, open, and/or manage a database in a specified location. This
   * method always returns very quickly. The database is not actually created or opened until one of
   * {@link #getWritableDatabase} or {@link #getReadableDatabase} is called.
   *
   * @param context to use to open or create the database
   * @param databaseName of the database file
   * @param storageDirectory to store the database file upon creation; caller must ensure that the
   *     specified absolute path is available and can be written to
   * @param factory to use for creating cursor objects, or null for the default
   * @param version number of the database (starting at 1); if the database is older, SQL file(s)
   *     contained within the application assets folder will be used to upgrade the database
   */
  public SQLiteAssetHelper(
      Context context,
      String databaseName,
      String storageDirectory,
      CursorFactory factory,
      int version) {
    super(context, databaseName, factory, version);

    if (version < 1) throw new IllegalArgumentException("Version must be >= 1, was " + version);
    if (databaseName == null) throw new IllegalArgumentException("Database name cannot be null");

    this.context = context;
    this.databaseName = databaseName;
    this.factory = factory;
    this.newVersion = version;

    this.assetPath = ASSET_DB_PATH + "/" + databaseName;
    if (storageDirectory != null) {
      this.databasePath = storageDirectory;
    } else {
      this.databasePath = context.getApplicationInfo().dataDir + "/databases";
    }
    this.upgradePathFormat = ASSET_DB_PATH + "/" + databaseName + "_upgrade_%s-%s.sql";
  }

  /**
   * Create a helper object to create, open, and/or manage a database in the application's default
   * private data directory. This method always returns very quickly. The database is not actually
   * created or opened until one of {@link #getWritableDatabase} or {@link #getReadableDatabase} is
   * called.
   *
   * @param context to use to open or create the database
   * @param databaseName of the database file
   * @param factory to use for creating cursor objects, or null for the default
   * @param version number of the database (starting at 1); if the database is older, SQL file(s)
   *     contained within the application assets folder will be used to upgrade the database
   */
  public SQLiteAssetHelper(
      Context context, String databaseName, CursorFactory factory, int version) {
    this(context, databaseName, null, factory, version);
  }

  /**
   * Create and/or open a database that will be used for reading and writing. The first time this is
   * called, the database will be extracted and copied from the application's assets folder.
   *
   * <p>Once opened successfully, the database is cached, so you can call this method every time you
   * need to write to the database. (Make sure to call {@link #close} when you no longer need the
   * database.) Errors such as bad permissions or a full disk may cause this method to fail, but
   * future attempts may succeed if the problem is fixed.
   *
   * <p>
   *
   * <p class="caution">Database upgrade may take a long time, you should not call this method from
   * the application main thread, including from {@link android.content.ContentProvider#onCreate
   * ContentProvider.onCreate()}.
   *
   * @throws SQLiteException if the database cannot be opened for writing
   * @return a read/write database object valid until {@link #close} is called
   */
  @Override
  public synchronized SQLiteDatabase getWritableDatabase() {
    if (database != null && database.isOpen() && !database.isReadOnly()) {
      return database; // The database is already open for business
    }

    if (isInitializing) {
      throw new IllegalStateException("getWritableDatabase called recursively");
    }

    // If we have a read-only database open, someone could be using it
    // (though they shouldn't), which would cause a lock to be held on
    // the file, and our attempts to open the database read-write would
    // fail waiting for the file lock.  To prevent that, we acquire the
    // lock on the read-only database, which shuts out other users.

    boolean success = false;
    SQLiteDatabase db = null;
    // if (mDatabase != null) mDatabase.lock();
    try {
      isInitializing = true;
      db = createOrOpenDatabase(false);

      int version = db.getVersion();

      // do force upgrade
      if (version != 0 && version < forcedUpgradeVersion) {
        db = createOrOpenDatabase(true);
        db.setVersion(newVersion);
        version = db.getVersion();
      }

      if (version != newVersion) {
        db.beginTransaction();
        try {
          if (version == 0) {
            onCreate(db);
          } else {
            if (version > newVersion) {
              Log.w(
                  SqliteAssetUtil.TAG,
                  "Can't downgrade read-only database from version "
                      + version
                      + " to "
                      + newVersion
                      + ": "
                      + db.getPath());
            }
            onUpgrade(db, version, newVersion);
          }
          db.setVersion(newVersion);
          db.setTransactionSuccessful();
        } finally {
          db.endTransaction();
        }
      }

      onOpen(db);
      success = true;
      return db;
    } finally {
      isInitializing = false;
      if (success) {
        if (database != null) {
          try {
            database.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
          // mDatabase.unlock();
        }
        database = db;
      } else {
        // if (mDatabase != null) mDatabase.unlock();
        if (db != null) db.close();
      }
    }
  }

  /**
   * Create and/or open a database. This will be the same object returned by {@link
   * #getWritableDatabase} unless some problem, such as a full disk, requires the database to be
   * opened read-only. In that case, a read-only database object will be returned. If the problem is
   * fixed, a future call to {@link #getWritableDatabase} may succeed, in which case the read-only
   * database object will be closed and the read/write object will be returned in the future.
   *
   * <p>
   *
   * <p class="caution">Like {@link #getWritableDatabase}, this method may take a long time to
   * return, so you should not call it from the application main thread, including from {@link
   * android.content.ContentProvider#onCreate ContentProvider.onCreate()}.
   *
   * @throws SQLiteException if the database cannot be opened
   * @return a database object valid until {@link #getWritableDatabase} or {@link #close} is called.
   */
  @Override
  public synchronized SQLiteDatabase getReadableDatabase() {
    if (database != null && database.isOpen()) {
      return database; // The database is already open for business
    }

    if (isInitializing) {
      throw new IllegalStateException("getReadableDatabase called recursively");
    }

    try {
      return getWritableDatabase();
    } catch (SQLiteException e) {
      if (databaseName == null) throw e; // Can't open a temp database read-only!
      Log.e(
          SqliteAssetUtil.TAG,
          "Couldn't open " + databaseName + " for writing (will try read-only):",
          e);
    }

    SQLiteDatabase db = null;
    try {
      isInitializing = true;
      String path = context.getDatabasePath(databaseName).getPath();
      db = SQLiteDatabase.openDatabase(path, factory, SQLiteDatabase.OPEN_READONLY);
      if (db.getVersion() != newVersion) {
        throw new SQLiteException(
            "Can't upgrade read-only database from version "
                + db.getVersion()
                + " to "
                + newVersion
                + ": "
                + path);
      }

      onOpen(db);
      Log.w(SqliteAssetUtil.TAG, "Opened " + databaseName + " in read-only mode");
      database = db;
      return database;
    } finally {
      isInitializing = false;
      if (db != null && db != database) db.close();
    }
  }

  /** Close any open database object. */
  @Override
  public synchronized void close() {
    if (isInitializing) throw new IllegalStateException("Closed during initialization");

    if (database != null && database.isOpen()) {
      database.close();
      database = null;
    }
  }

  @Override
  public final void onConfigure(SQLiteDatabase db) {
    // not supported!
  }

  @Override
  public final void onCreate(SQLiteDatabase db) {
    // do nothing - createOrOpenDatabase() is called in
    // getWritableDatabase() to handle database creation.
  }

  @SuppressWarnings("Java8ListSort")
  @Override
  public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    Log.w(
        SqliteAssetUtil.TAG,
        "Upgrading database "
            + databaseName
            + " from version "
            + oldVersion
            + " to "
            + newVersion
            + "...");

    ArrayList<String> paths = new ArrayList<>();
    getUpgradeFilePaths(oldVersion, newVersion - 1, newVersion, paths);

    if (paths.isEmpty()) {
      Log.e(SqliteAssetUtil.TAG, "no upgrade script path from " + oldVersion + " to " + newVersion);
      throw new SQLiteAssetException(
          "no upgrade script path from " + oldVersion + " to " + newVersion);
    }

    Collections.sort(paths, new VersionComparator());
    for (String path : paths) {
      try {
        Log.w(SqliteAssetUtil.TAG, "processing upgrade: " + path);
        InputStream is = context.getAssets().open(path);
        String sql = ScriptUtil.convertStreamToString(is);
        if (sql != null) {
          List<String> commandList = ScriptUtil.splitSqlScript(sql, ';');
          for (String cmd : commandList) {
            // Log.d(TAG, "cmd=" + cmd);
            if (cmd.trim().length() > 0) {
              db.execSQL(cmd);
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    Log.w(
        SqliteAssetUtil.TAG,
        "Successfully upgraded database "
            + databaseName
            + " from version "
            + oldVersion
            + " to "
            + newVersion);
  }

  @Override
  public final void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    // not supported!
  }

  /**
   * Bypass the upgrade process (for each increment up to a given version) and simply overwrite the
   * existing database with the supplied asset file.
   *
   * @param version bypass upgrade up to this version number - should never be greater than the
   *     latest database version.
   * @deprecated use {@link #setForcedUpgrade} instead.
   */
  @Deprecated
  public void setForcedUpgradeVersion(int version) {
    setForcedUpgrade(version);
  }

  /**
   * Bypass the upgrade process (for each increment up to a given version) and simply overwrite the
   * existing database with the supplied asset file.
   *
   * @param version bypass upgrade up to this version number - should never be greater than the
   *     latest database version.
   */
  public void setForcedUpgrade(int version) {
    forcedUpgradeVersion = version;
  }

  /**
   * Bypass the upgrade process for every version increment and simply overwrite the existing
   * database with the supplied asset file.
   */
  @SuppressWarnings("unused")
  public void setForcedUpgrade() {
    setForcedUpgrade(newVersion);
  }

  private SQLiteDatabase createOrOpenDatabase(boolean force) throws SQLiteAssetException {

    // test for the existence of the db file first and don't attempt open
    // to prevent the error trace in log on API 14+
    SQLiteDatabase db = null;
    File file = new File(databasePath + "/" + databaseName);
    if (file.exists()) {
      db = returnDatabase();
    }
    // SQLiteDatabase db = returnDatabase();

    if (db != null) {
      // database already exists
      if (force) {
        Log.w(SqliteAssetUtil.TAG, "forcing database upgrade!");
        SqliteAssetUtil.copyDatabaseFromAssets(assetPath, databasePath, databaseName, context);
        db = returnDatabase();
      }
    } else {
      // database does not exist, copy it from assets and return it
      SqliteAssetUtil.copyDatabaseFromAssets(assetPath, databasePath, databaseName, context);
      db = returnDatabase();
    }
    return db;
  }

  private SQLiteDatabase returnDatabase() {
    try {
      SQLiteDatabase db =
          SQLiteDatabase.openDatabase(
              databasePath + "/" + databaseName, factory, SQLiteDatabase.OPEN_READWRITE);
      Log.i(SqliteAssetUtil.TAG, "successfully opened database " + databaseName);
      return db;
    } catch (SQLiteException e) {
      Log.w(
          SqliteAssetUtil.TAG, "could not open database " + databaseName + " - " + e.getMessage());
      return null;
    }
  }

  private InputStream getUpgradeSQLStream(int oldVersion, int newVersion) {
    String path = String.format(upgradePathFormat, oldVersion, newVersion);
    try {
      return context.getAssets().open(path);
    } catch (IOException e) {
      Log.w(SqliteAssetUtil.TAG, "missing database upgrade script: " + path);
      return null;
    }
  }

  private void getUpgradeFilePaths(int baseVersion, int start, int end, ArrayList<String> paths) {

    int a;
    int b;

    InputStream is = getUpgradeSQLStream(start, end);
    if (is != null) {
      String path = String.format(upgradePathFormat, start, end);
      paths.add(path);
      // Log.d(TAG, "found script: " + path);
      a = start - 1;
      b = start;
    } else {
      a = start - 1;
      b = end;
    }

    if (a < baseVersion) {
      //noinspection UnnecessaryReturnStatement
      return;
    } else {
      getUpgradeFilePaths(baseVersion, a, b, paths); // recursive call
    }
  }
}
