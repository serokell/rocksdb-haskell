{-# LANGUAGE BinaryLiterals      #-}
{-# LANGUAGE OverloadedStrings   #-}

module Main where

import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (MonadResource, runResourceT)
import           Data.Char                    (chr)
import           Data.Default                 (def)
import           System.IO.Temp               (withSystemTempDirectory)

import           Database.RocksDB             (Compression (..), DB, compression,
                                               createIfMissing, defaultOptions, get, open,
                                               put)

import           Test.Hspec                   (describe, hspec, it, shouldReturn)
import           Test.QuickCheck              (Arbitrary (..), elements, generate, listOf)

initializeDB :: MonadResource m => FilePath -> m DB
initializeDB path =
    open
        path
        defaultOptions
        {createIfMissing = True, compression = NoCompression}

newtype HardString = HardString {getHardString :: FilePath}
  deriving (Show, Eq)

instance Arbitrary HardString where
    arbitrary =
        HardString <$> (listOf . elements . map chr $ notlatin)
       where
         -- This is not intended to be an exhaustive of nonlatin Unicode
         -- alphabets/syllabaries
         notlatin = japanese ++ russian
         japanese = [0x3040..0x309F] ++ [0x30A0..0x30FF]
         russian  = [0x0410..0x044F]

main :: IO ()
main =  hspec $ do

  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        db <- initializeDB path
        put db def "zzz" "zzz"
        get db def "zzz"
      `shouldReturn` (Just "zzz")

    it "should put items into a database whose filepath has unicode characters and\
       \ retrieve them" $  do
      runResourceT $ withSystemTempDirectory "rocksdb" $ \path -> do
        unicode <- getHardString <$> liftIO (generate arbitrary)
        db <- initializeDB $ path ++ unicode
        put db def "zzz" "zzz"
        get db def "zzz"
      `shouldReturn` (Just "zzz")
