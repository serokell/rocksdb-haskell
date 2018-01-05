{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Concurrent           (threadDelay)
import           Control.Concurrent.Async     (race_)
import           Control.Exception            (ErrorCall(..))
import           Control.Monad                (when)
import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (runResourceT)
import           Data.Default                 (def)
import           System.Directory             (createDirectoryIfMissing, doesDirectoryExist, removeDirectoryRecursive)
import           System.FilePath              ((</>))
import           System.IO.Error              (ioeGetErrorType, isUserErrorType, ioeGetErrorString)
import           System.IO.Temp               (withSystemTempDirectory)

import           Database.RocksDB             (Compression (..), Options (..),
                                               createIfMissing, defaultOptions, get, withDB, open, openBracket, openAutoclose,
                                               put, close, finalize)

import           Test.Hspec                   (describe, hspec, it, shouldReturn, shouldThrow)
import           Test.QuickCheck              (Arbitrary (..), UnicodeString (..),
                                               generate)

testOpenOptions :: Options
testOpenOptions =
  defaultOptions
    {createIfMissing = True, compression = NoCompression}

main :: IO ()
main =  hspec $ do

  describe "Basic DB Functionality" $ do

    it "should put items into the database and retrieve them (withDB way)" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        withDB path testOpenOptions $ \db -> do
          put db def "zzz" "zzz"
          val <- get db def "zzz"
          return val
      `shouldReturn` (Just "zzz")

    it "should put items into the database and retrieve them (non-asysnc-safe way)" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        db <- open path testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        close db
        return val
      `shouldReturn` (Just "zzz")

    it "should put items into the database and retrieve them (MonadResource way)" $ do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        (_, db) <- openBracket path testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        close db -- TODO remove; this should make the test fail, but it doesn't because errors in finalizers run by runResourceT don't seem to bubble up
        return val
      `shouldReturn` (Just "zzz")

    it "should put items into the database and retrieve them (autoclose way)" $ do
      -- [Note: Unique tmp dirs for autoclose without finalize]
      -- We need to use a different temp dir prefix here that we don't reuse
      -- anywhere else, because the GC can delay `close`ing the DB for
      -- arbitrarily long, and RocksDB has a lock check that would make
      -- using this directory again fail before `close` is called;
      -- so later tests using the same dir would fail.
      -- See comment on `openAutoclose`.
      withSystemTempDirectory "rocksdb-autoclose-uniquedir1" $ \path -> do
        db <- openAutoclose path testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
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

      (do
        db <- open path testOpenOptions
        put db def "zzz" "zzz"
        get db def "zzz") `shouldReturn` (Just "zzz")
      -- We purposely don't `close` the DB above.

      removeDirectoryRecursive path

      (do
        db <- open path testOpenOptions
        put db def "zzz" "zzz"
        get db def "zzz") `shouldThrow` \ioe ->
          isUserErrorType (ioeGetErrorType ioe) &&
          ioeGetErrorString ioe `elem`
            [ "open: IO error: lock " ++ path ++ "/LOCK: No locks available"
            , "open: IO error: While lock file: " ++ path ++ "/LOCK: No locks available"
            ]

    it "should put items into a database whose filepath has unicode characters and\
       \ retrieve them" $ do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        unicode <- getUnicodeString <$> liftIO (generate arbitrary)
        (_, db) <- openBracket (path </> "unicode-randomdir-" ++ unicode) testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        return val
      `shouldReturn` (Just "zzz")

  describe "double-close detection" $ do

    it "should detect manual close within withDB" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        withDB path testOpenOptions $ \db -> do
          put db def "zzz" "zzz"
          val <- get db def "zzz"
          close db
          return val
      `shouldThrow` (\(ErrorCall str) -> str == "haskell-rocksdb ensureOpenAndClose: Caller BUG: DB is closed")

    it "should detect manual close after openAutoclose" $ do
      -- See Note "Unique tmp dirs for autoclose without finalize".
      withSystemTempDirectory "rocksdb-autoclose-uniquedir2" $ \path -> do
        db <- openAutoclose path testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        close db
        return val
      `shouldThrow` (\(ErrorCall str) -> str == "haskell-rocksdb close: Caller BUG: DB opened with 'openAutoclose' must not be closed manually")

    it "should not trigger after orderly manual finalize" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        db <- openAutoclose path testOpenOptions
        put db def "zzz" "zzz"
        _val <- get db def "zzz"
        finalize db :: IO ()

  describe "multi-thread crash checks" $ do

    it "should not segfault on use-after-close, should error instead" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        db <- open path testOpenOptions
        race_
          (do
            put db def "key" "value1"
            close db
            threadDelay 2000000
          )
          (do
            threadDelay 1000000
            put db def "key" "value2"
            close db
          )
        `shouldThrow` (\(ErrorCall str) -> str == "haskell-rocksdb ensureOpen: Caller BUG: DB is closed")
