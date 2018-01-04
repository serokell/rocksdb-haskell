{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Monad                (when)
import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (runResourceT)
import           Data.Default                 (def)
import           System.Directory             (createDirectoryIfMissing, doesDirectoryExist, removeDirectoryRecursive)
import           System.FilePath              ((</>))
import           System.IO.Error              (ioeGetErrorType, isUserErrorType, ioeGetErrorString)
import           System.IO.Temp               (withSystemTempDirectory)

import           Database.RocksDB             (Compression (..), DB, compression,
                                               createIfMissing, defaultOptions, get, open,
                                               put, close)

import           Test.Hspec                   (describe, hspec, it, shouldReturn, shouldThrow)
import           Test.QuickCheck              (Arbitrary (..), UnicodeString (..),
                                               generate)

initializeDB :: MonadIO m => FilePath -> m DB
initializeDB path =
    open
        path
        defaultOptions
        {createIfMissing = True, compression = NoCompression}

main :: IO ()
main =  hspec $ do

  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        db <- initializeDB path
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        close db
        return val
      `shouldReturn` (Just "zzz")

    it "does weird global singleton string matching stuff for double-locking warnings" $ do
      -- This test will fail when RocksDB fixes this issue
      -- (see https://stackoverflow.com/questions/37310588/rocksdb-io-error-lock-no-locks-available#comment83145041_37312033).
      -- It exists so that we get notified when that fixing happens.
      let path = "/tmp/tmpdir"

      exists <- doesDirectoryExist path
      when exists $ removeDirectoryRecursive path

      createDirectoryIfMissing True path

      (runResourceT $ do
        db <- initializeDB path
        put db def "zzz" "zzz"
        get db def "zzz") `shouldReturn` (Just "zzz")
      -- We purposely don't `close` the DB above.

      removeDirectoryRecursive path

      (runResourceT $ do
        db <- initializeDB path
        put db def "zzz" "zzz"
        get db def "zzz") `shouldThrow` \ioe ->
          isUserErrorType (ioeGetErrorType ioe) &&
          ioeGetErrorString ioe == ("open: IO error: lock " ++ path ++ "/LOCK: No locks available")

    it "should put items into a database whose filepath has unicode characters and\
       \ retrieve them" $  do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        unicode <- getUnicodeString <$> liftIO (generate arbitrary)
        db <- initializeDB $ path </> "unicode-randomdir-" ++ unicode
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        close db
        return val
      `shouldReturn` (Just "zzz")
