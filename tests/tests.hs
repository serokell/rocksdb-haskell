{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (runResourceT)
import           Data.Default                 (def)
import           System.FilePath              ((</>))
import           System.IO.Temp               (withSystemTempDirectory)

import           Database.RocksDB             (Compression (..), DB, compression,
                                               createIfMissing, defaultOptions, get, open,
                                               put, close)

import           Test.Hspec                   (describe, hspec, it, shouldReturn)
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
