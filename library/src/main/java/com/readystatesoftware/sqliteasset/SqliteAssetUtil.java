package com.readystatesoftware.sqliteasset;

import android.content.Context;
import android.content.res.AssetManager;
import android.util.Log;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

class SqliteAssetUtil {

  static final String TAG = SQLiteAssetHelper.class.getSimpleName();

  private SqliteAssetUtil() {}

  static void copyDatabaseFromAssets(
      String assetPath, String databasePath, String databaseName, Context context)
      throws SQLiteAssetException {
    AssetManager assets = context.getAssets();
    copyInternal(assetPath, databasePath, databaseName, assets);
  }

  @SuppressWarnings({"IOStreamConstructor", "ResultOfMethodCallIgnored"})
  private static void copyInternal(
      String assetPath, String databasePath, String databaseName, AssetManager assets) {
    Log.w(TAG, "copying database from assets...");

    String dest = databasePath + "/" + databaseName;
    InputStream is;
    boolean isZip = false;

    try {
      // try uncompressed
      is = assets.open(assetPath);
    } catch (IOException e) {
      // try zip
      try {
        is = assets.open(assetPath + ".zip");
        isZip = true;
      } catch (IOException e2) {
        // try gzip
        try {
          is = assets.open(assetPath + ".gz");
        } catch (IOException e3) {
          SQLiteAssetException se =
              new SQLiteAssetException(
                  "Missing "
                      + assetPath
                      + " file (or .zip, .gz archive) in assets, or target folder not writable");
          se.setStackTrace(e3.getStackTrace());
          throw se;
        }
      }
    }

    try {
      File f = new File(databasePath + "/");
      if (!f.exists()) {
        f.mkdir();
      }
      if (isZip) {
        ZipInputStream zis = ScriptUtil.getFileFromZip(is);
        if (zis == null) {
          throw new SQLiteAssetException("Archive is missing a SQLite database file");
        }
        ScriptUtil.writeExtractedFileToDisk(zis, new FileOutputStream(dest));
      } else {
        ScriptUtil.writeExtractedFileToDisk(is, new FileOutputStream(dest));
      }

      Log.w(TAG, "database copy complete");

    } catch (IOException e) {
      SQLiteAssetException se =
          new SQLiteAssetException("Unable to write " + dest + " to data directory");
      se.setStackTrace(e.getStackTrace());
      throw se;
    }
  }
}
