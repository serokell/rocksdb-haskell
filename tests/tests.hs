{-# LANGUAGE CPP                 #-}
{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Concurrent           (threadDelay)
import           Control.Concurrent.Async     (race_)
import           Control.Exception            (ErrorCall(..), fromException)
import           Control.Monad                (when)
import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (runResourceTChecked, allocate, ResourceCleanupException(..))
import           Data.Default                 (def)
import           System.Directory             (createDirectoryIfMissing, doesDirectoryExist, removeDirectoryRecursive)
import           System.FilePath              ((</>))
import           System.IO.Error              (ioeGetErrorType, isUserErrorType, ioeGetErrorString)
import           System.IO.Temp               (withSystemTempDirectory)

import           Database.RocksDB             (Compression (..), Options (..),
                                               createIfMissing, defaultOptions, get, withDB, open, openBracket,
                                               put, close)

import           Test.Hspec                   (describe, hspec, it, shouldReturn, shouldThrow)
import           Test.QuickCheck              (Arbitrary (..), UnicodeString (..),
                                               generate)

testOpenOptions :: Options
testOpenOptions =
  defaultOptions
    {createIfMissing = True, compression = NoCompression}

main :: IO ()
main =  hspec $ do

  describe "Underlying library behaviour" $ do

    it "runResourceTChecked doesn't ignore errors in finalizers" $ do
      -- Added to `resourcet` so that our use case here is satisfied:
      --   https://github.com/snoyberg/conduit/pull/347
      runResourceTChecked $ do
        _ <- allocate (return 'x') (\_x -> putStrLn "finalizer running" >> error "allocate finalizer fail")
        return ()
      `shouldThrow` \e -> case e of
          ResourceCleanupException ex [] -> case fromException ex of
            Just (ErrorCall "allocate finalizer fail") -> True
            _ -> False
          _ -> False

  describe "Basic DB Functionality" $ do

    it "should put items into the database and retrieve them (withDB way)" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        withDB path testOpenOptions $ \db -> do
          put db def "zzz" "zzz"
          val <- get db def "zzz"
          return val
      `shouldReturn` (Just "zzz")

    it "should put items into the database and retrieve them (non-async-safe way)" $ do
      withSystemTempDirectory "rocksdb" $ \path -> do
        db <- open path testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        close db
        return val
      `shouldReturn` (Just "zzz")

    it "should put items into the database and retrieve them (MonadResource way)" $ do
      runResourceTChecked $ withSystemTempDirectory "rocksdb" $ \path -> do
        (_, db) <- openBracket path testOpenOptions
        put db def "zzz" "zzz"
        val <- get db def "zzz"
        return val
      `shouldReturn` (Just "zzz")

    -- This test doesn't apply to Windows because there we can't delete the
    -- directory while a file in it is open.
#if !defined(mingw32_HOST_OS)
    it "does weird global singleton string matching stuff for double-locking warnings" $ do
      -- This test will fail when RocksDB fixes this issue
      -- (see https://stackoverflow.com/questions/37310588/rocksdb-io-error-lock-no-locks-available#comment83145041_37312033).
      -- It exists so that we get notified when that fixing happens.
      let path = "/tmp/tmpdir"

      exists <- doesDirectoryExist path
      when exists $ removeDirectoryRecursive path

      createDirectoryIfMissing True path

      db <- open path testOpenOptions
      (do
        put db def "zzz" "zzz"
        get db def "zzz") `shouldReturn` (Just "zzz")
      -- We purposely don't `close` the DB above.

      removeDirectoryRecursive path

      open path testOpenOptions `shouldThrow` \ioe ->
        isUserErrorType (ioeGetErrorType ioe) &&
        ioeGetErrorString ioe `elem`
          [ "open: IO error: lock " ++ path ++ "/LOCK: No locks available"
          , "open: IO error: While lock file: " ++ path ++ "/LOCK: No locks available"
          ]

      -- Close first DB now so that it doesn't memory-leak.
      close db
#endif

    it "should put items into a database whose filepath has unicode characters and retrieve them" $ do
      runResourceTChecked $ withSystemTempDirectory "rocksdb" $ \path -> do
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
