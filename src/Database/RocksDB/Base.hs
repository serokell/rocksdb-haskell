{-# LANGUAGE CPP           #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TupleSections #-}

-- |
-- Module      : Database.RocksDB.Base
-- Copyright   : (c) 2012-2013 The leveldb-haskell Authors
--               (c) 2014 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : mail@agrafix.net
-- Stability   : experimental
-- Portability : non-portable
--
-- RocksDB Haskell binding.
--
-- The API closely follows the C-API of RocksDB.
-- For more information, see: <http://agrafix.net>

module Database.RocksDB.Base
    ( -- * Exported Types
      DB
    , BatchOp (..)
    , Comparator (..)
    , Compression (..)
    , Options (..)
    , ReadOptions (..)
    , Snapshot
    , WriteBatch
    , WriteOptions (..)
    , Range

    -- * Defaults
    , defaultOptions
    , defaultReadOptions
    , defaultWriteOptions

    -- * Basic Database Manipulations
    , withDB
    , open
    , openAutoclose
    , openBracket
    , finalize
    , close
    , put
    , putBinaryVal
    , putBinary
    , delete
    , write
    , get
    , getBinary
    , getBinaryVal
    , withSnapshot
    , withSnapshotBracket
    , createSnapshot
    , releaseSnapshot

    -- * Filter Policy / Bloom Filter
    , FilterPolicy (..)
    , BloomFilter
    , createBloomFilter
    , releaseBloomFilter
    , bloomFilter

    -- * Administrative Functions
    , Property (..), getProperty
    , destroy
    , repair
    , approximateSize

    -- * Iteration
    , module Database.RocksDB.Iterator
    ) where

import           Control.Applicative          ((<$>))
import           Control.Exception            (bracket, bracketOnError, finally, mask_)
import           Control.Monad                (liftM, when)

import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Resource (MonadResource (..), ReleaseKey, allocate,
                                               release)
import           Data.Binary                  (Binary)
import qualified Data.Binary                  as Binary
import           Data.ByteString              (ByteString)
import           Data.ByteString.Internal     (ByteString (..))
import qualified Data.ByteString.Lazy         as BSL
import           Data.IORef                   (newIORef, atomicWriteIORef)
import           Foreign
import           Foreign.C.String             (CString, withCString)
import           GHC.Stack                    (HasCallStack)
import           System.Directory             (createDirectoryIfMissing)

import           Database.RocksDB.C
import           Database.RocksDB.Internal
import           Database.RocksDB.Iterator
import           Database.RocksDB.Types

import qualified Data.ByteString              as BS
import qualified Data.ByteString.Unsafe       as BU

import qualified GHC.Foreign                  as GHC
import qualified GHC.IO.Encoding              as GHC

-- | Create a 'BloomFilter'
bloomFilter :: MonadResource m => Int -> m BloomFilter
bloomFilter i =
    snd <$> allocate (createBloomFilter i)
                      releaseBloomFilter

-- | Open a database
--
-- The returned handle will automatically be released when the enclosing
-- 'runResourceT' terminates.
openBracket :: MonadResource m => FilePath -> Options -> m (ReleaseKey, DB)
openBracket path opts = allocate (open path opts) close
{-# INLINE openBracket #-}
-- Note that in the above, we don't get double-close detection
-- (see comment on `DB`), because apparently `runResourceT` swallows
-- any `error` that we may raise in the cleanup function given to
-- allocate. So if our double-close detection `error` triggers in
-- `close`, the user will not see it.

-- | Run an action with a snapshot of the database.
--
-- The snapshot will be released when the action terminates or throws an
-- exception. Note that this function is provided for convenience and does not
-- prevent the 'Snapshot' handle to escape. It will, however, be invalid after
-- this function returns and should not be used anymore.
withSnapshotBracket :: MonadResource m => DB -> (Snapshot -> m a) -> m a
withSnapshotBracket db f = do
    (rk, snap) <- createSnapshotBracket db
    res <- f snap
    release rk
    return res

-- | Create a snapshot of the database.
--
-- The returned 'Snapshot' will be released automatically when the enclosing
-- 'runResourceT' terminates. It is recommended to use 'createSnapshot'' instead
-- and release the resource manually as soon as possible.
-- Can be released early.
createSnapshotBracket :: MonadResource m => DB -> m (ReleaseKey, Snapshot)
createSnapshotBracket db = allocate (createSnapshot db) (releaseSnapshot db)

-- | Open a database.
--
-- The returned handle must be released with 'close', or
-- it will memory leak.
--
-- The `ForeignPtr` in the returned `DB` does not have a
-- finalizer attached.
--
-- This function is not async-exception-safe.
-- If interrupted by an async exceptions, the resources acquired
-- by this function will memory-leak.
-- Thus it should be used only within `bracket` or a similar function
-- so that a finalizer calling `close` can be attached while async
-- exceptions are disabled.
open :: (HasCallStack, MonadIO m) => FilePath -> Options -> m DB
open path opts = liftIO $ bracketOnError initializeOpts finalizeOpts mkDB
    where
# ifdef mingw32_HOST_OS
        initializeOpts =
            (, ()) <$> mkOpts opts
        finalizeOpts (opts', ()) =
            freeOpts opts'
# else
        initializeOpts = do
            opts' <- mkOpts opts
            -- With LC_ALL=C, two things happen:
            --   * rocksdb can't open a database with unicode in path;
            --   * rocksdb can't create a folder properly.
            -- So, we create the folder by ourselves, and for thart we
            -- need to set the encoding we're going to use. On Linux
            -- it's almost always UTC-8.
            oldenc <- GHC.getFileSystemEncoding
            when (createIfMissing opts) $
                GHC.setFileSystemEncoding GHC.utf8
            pure (opts', oldenc)
        finalizeOpts (opts', oldenc) = do
            freeOpts opts'
            GHC.setFileSystemEncoding oldenc
# endif
        mkDB (opts'@(Options' opts_ptr _ _), _) = do
            when (createIfMissing opts) $
                createDirectoryIfMissing True path
            withFilePath path $ \path_ptr -> do
                bracketOnError
                    (do
                        db_ptr <- throwIfErr "open" $ c_rocksdb_open opts_ptr path_ptr
                        statusRef <- newIORef Open
                        db_fptr <- newForeignPtr_ db_ptr
                        return $ DB
                            { db_fptr
                            , dbOptions = opts'
                            , dbHandleStatusRef = statusRef
                            , dbAutoclose = False
                            }
                    )
                    (\DB{ db_fptr, dbOptions, dbHandleStatusRef } -> do
                        atomicWriteIORef dbHandleStatusRef Closed
                        finalizeForeignPtr db_fptr `finally` freeOpts dbOptions
                    )
                    return

-- | Like open, but the DB will be auto-closed when the
-- returned `DB` gets garbage-collected.
--
-- This function is async-exception safe.
--
-- You must not call `close` on the returned handle.
--
-- If you want to promptly close the handle, use `finalize`
-- instead. After you have called `finalize` on the handle,
-- you must no longer use it for any DB operation.
--
-- Note it is still not recommended to use this without
-- explicit `finalize` because if you want to open the same
-- database again from the same process, you may run into the
-- issue that the GC has not yet run `finalize` for you, and
-- RocksDB has a global state of what DBs are opened
-- (see https://stackoverflow.com/questions/37310588/rocksdb-io-error-lock-no-locks-available#comment83145041_37312033),
-- so code like the following may fail with a lock error:
--
-- > db1 <- openAutoclose "/path/to/db" ...
-- > ...
-- > -- db1 goes out of scope here
-- > db2 <- openAutoclose "/path/to/db" ...
-- > -- ^ the above may fail with a lock error if the GC hasn't run
--
-- This can be fixed by running `finalize db1` when you're
-- really done with the DB.
--
-- The safest way to do so is to use a bracketing function like `withDB`,
-- `openBracket` which uses `MonadResource` to close the DB promptly.
openAutoclose :: (HasCallStack, MonadIO m) => FilePath -> Options -> m DB
openAutoclose path opts = liftIO $ do
    mask_ $ do
        db <- open path opts
        addForeignPtrFinalizer c_rocksdb_close_funptr (db_fptr db)
        return $ db{ dbAutoclose = True }

-- | The simplest and safest way to use a DB.
--
-- The DB is closed when this function returns.
--
-- You must not return the DB out of the inner scope.
--
-- You must not call `close` on the DB.
--
-- You may call `finalize` on the DB to close it before
-- `withDB` returns if you know that no
-- other DB operation will be performed afterwards,
-- or is performing in another thread concurrently.
withDB :: (HasCallStack) => FilePath -> Options -> (DB -> IO a) -> IO a
withDB path opts f = do
    bracket
        (open path opts)
        close
        f

-- | Close a database.
--
-- The handle will be invalid after calling this action and should no
-- longer be used.
-- If it is used, with the current implementation of RocksDB,
-- the program will SEGFAULT.
-- If another RocksDB operation is still running in another thread while
-- close() is function is called, this may results in crashes or data loss.
close :: (MonadIO m, HasCallStack) => DB -> m ()
close db@DB{ db_fptr, dbOptions, dbAutoclose } = liftIO $ do
    when dbAutoclose $ do
        error $ "haskell-rocksdb close: Caller BUG: DB opened with 'openAutoclose' must not be closed manually"
    ensureOpenAndClose db
    withForeignPtr db_fptr $ \db_ptr -> do
        c_rocksdb_close db_ptr `finally` freeOpts dbOptions

-- | Like `close`, but it is not an error to call this function
-- multiple times on the same `DB` handle.
--
-- This function should be called on DB handles opened with
-- `openAutoclose`. It has no effect on handles opened with
-- plain `open`.
finalize :: MonadIO m => DB -> m ()
finalize DB{ db_fptr } = liftIO $
    finalizeForeignPtr db_fptr

-- | Run an action with a 'Snapshot' of the database.
withSnapshot :: (MonadIO m, HasCallStack) => DB -> (Snapshot -> IO a) -> m a
withSnapshot db act = liftIO $
    bracket (createSnapshot db) (releaseSnapshot db) act

-- | Create a snapshot of the database.
--
-- The returned 'Snapshot' should be released with 'releaseSnapshot'.
createSnapshot :: (MonadIO m, HasCallStack) => DB -> m Snapshot
createSnapshot db@DB{ db_fptr } = liftIO $ do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        Snapshot <$> c_rocksdb_create_snapshot db_ptr

-- | Release a snapshot.
--
-- The handle will be invalid after calling this action and should no
-- longer be used.
releaseSnapshot :: (MonadIO m, HasCallStack) => DB -> Snapshot -> m ()
releaseSnapshot db@DB{ db_fptr } (Snapshot snap) = liftIO $ do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        c_rocksdb_release_snapshot db_ptr snap

-- | Get a DB property.
getProperty :: (MonadIO m, HasCallStack) => DB -> Property -> m (Maybe ByteString)
getProperty db@DB{ db_fptr } p = liftIO $ do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr -> do
        withCString (prop p) $ \prop_ptr -> do
            val_ptr <- c_rocksdb_property_value db_ptr prop_ptr
            if val_ptr == nullPtr
                then return Nothing
                else do res <- Just <$> BS.packCString val_ptr
                        freeCString val_ptr
                        return res
    where
        prop (NumFilesAtLevel i) = "rocksdb.num-files-at-level" ++ show i
        prop Stats               = "rocksdb.stats"
        prop SSTables            = "rocksdb.sstables"

-- | Destroy the given RocksDB database.
destroy :: MonadIO m => FilePath -> Options -> m ()
destroy path opts = liftIO $ bracket (mkOpts opts) freeOpts destroy'
    where
        destroy' (Options' opts_ptr _ _) =
            withFilePath path $ \path_ptr ->
                throwIfErr "destroy" $ c_rocksdb_destroy_db opts_ptr path_ptr

-- | Repair the given RocksDB database.
repair :: MonadIO m => FilePath -> Options -> m ()
repair path opts = liftIO $ bracket (mkOpts opts) freeOpts repair'
    where
        repair' (Options' opts_ptr _ _) =
            withFilePath path $ \path_ptr ->
                throwIfErr "repair" $ c_rocksdb_repair_db opts_ptr path_ptr


-- TODO: support [Range], like C API does
type Range  = (ByteString, ByteString)

-- | Inspect the approximate sizes of the different levels.
approximateSize :: (MonadIO m, HasCallStack) => DB -> Range -> m Int64
approximateSize db@DB{ db_fptr } (from, to) = liftIO $ do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        BU.unsafeUseAsCStringLen from $ \(from_ptr, flen) ->
        BU.unsafeUseAsCStringLen to   $ \(to_ptr, tlen)   ->
        withArray [from_ptr]          $ \from_ptrs        ->
        withArray [intToCSize flen]   $ \flen_ptrs        ->
        withArray [to_ptr]            $ \to_ptrs          ->
        withArray [intToCSize tlen]   $ \tlen_ptrs        ->
        allocaArray 1                 $ \size_ptrs        -> do
            c_rocksdb_approximate_sizes db_ptr 1
                                        from_ptrs flen_ptrs
                                        to_ptrs tlen_ptrs
                                        size_ptrs
            liftM head $ peekArray 1 size_ptrs >>= mapM toInt64

    where
        toInt64 = return . fromIntegral

putBinaryVal :: (MonadIO m, Binary v) => DB -> WriteOptions -> ByteString -> v -> m ()
putBinaryVal db wopts key val = put db wopts key (binaryToBS val)

putBinary :: (MonadIO m, Binary k, Binary v) => DB -> WriteOptions -> k -> v -> m ()
putBinary db wopts key val = put db wopts (binaryToBS key) (binaryToBS val)

-- | Write a key/value pair.
put :: (MonadIO m, HasCallStack) => DB -> WriteOptions -> ByteString -> ByteString -> m ()
put db@DB{ db_fptr } opts key value = liftIO $ withCWriteOpts opts $ \opts_ptr -> do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        BU.unsafeUseAsCStringLen key   $ \(key_ptr, klen) ->
        BU.unsafeUseAsCStringLen value $ \(val_ptr, vlen) ->
            throwIfErr "put"
                $ c_rocksdb_put db_ptr opts_ptr
                                key_ptr (intToCSize klen)
                                val_ptr (intToCSize vlen)

getBinaryVal :: (Binary v, MonadIO m) => DB -> ReadOptions -> ByteString -> m (Maybe v)
getBinaryVal db ropts key  = fmap bsToBinary <$> get db ropts key

getBinary :: (MonadIO m, Binary k, Binary v) => DB -> ReadOptions -> k -> m (Maybe v)
getBinary db ropts key = fmap bsToBinary <$> get db ropts (binaryToBS key)

-- | Read a value by key.
get :: (MonadIO m, HasCallStack) => DB -> ReadOptions -> ByteString -> m (Maybe ByteString)
get db@DB{ db_fptr } opts key = liftIO $ withCReadOpts opts $ \opts_ptr -> do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
        alloca                       $ \vlen_ptr -> do
            val_ptr <- throwIfErr "get" $
                c_rocksdb_get db_ptr opts_ptr key_ptr (intToCSize klen) vlen_ptr
            vlen <- peek vlen_ptr
            if val_ptr == nullPtr
                then return Nothing
                else do
                    res' <- Just <$> BS.packCStringLen (val_ptr, cSizeToInt vlen)
                    freeCString val_ptr
                    return res'

-- | Delete a key/value pair.
delete :: (MonadIO m, HasCallStack) => DB -> WriteOptions -> ByteString -> m ()
delete db@DB{ db_fptr } opts key = liftIO $ withCWriteOpts opts $ \opts_ptr -> do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
            throwIfErr "delete"
                $ c_rocksdb_delete db_ptr opts_ptr key_ptr (intToCSize klen)

-- | Perform a batch mutation.
write :: (MonadIO m, HasCallStack) => DB -> WriteOptions -> WriteBatch -> m ()
write db@DB{ db_fptr } opts batch = liftIO $ withCWriteOpts opts $ \opts_ptr -> do
    ensureOpen db
    withForeignPtr db_fptr $ \db_ptr ->
        bracket c_rocksdb_writebatch_create c_rocksdb_writebatch_destroy $ \batch_ptr -> do

        mapM_ (batchAdd batch_ptr) batch

        throwIfErr "write" $ c_rocksdb_write db_ptr opts_ptr batch_ptr

    -- ensure @ByteString@s (and respective shared @CStringLen@s) aren't GC'ed
    -- until here
    mapM_ (liftIO . touch) batch

    where
        batchAdd batch_ptr (Put key val) =
            BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
            BU.unsafeUseAsCStringLen val $ \(val_ptr, vlen) ->
                c_rocksdb_writebatch_put batch_ptr
                                         key_ptr (intToCSize klen)
                                         val_ptr (intToCSize vlen)

        batchAdd batch_ptr (Del key) =
            BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
                c_rocksdb_writebatch_delete batch_ptr key_ptr (intToCSize klen)

        touch (Put (PS p _ _) (PS p' _ _)) = do
            touchForeignPtr p
            touchForeignPtr p'

        touch (Del (PS p _ _)) = touchForeignPtr p

createBloomFilter :: MonadIO m => Int -> m BloomFilter
createBloomFilter i = do
    let i' = fromInteger . toInteger $ i
    fp_ptr <- liftIO $ c_rocksdb_filterpolicy_create_bloom i'
    return $ BloomFilter fp_ptr

releaseBloomFilter :: MonadIO m => BloomFilter -> m ()
releaseBloomFilter (BloomFilter fp) = liftIO $ c_rocksdb_filterpolicy_destroy fp

binaryToBS :: Binary v => v -> ByteString
binaryToBS x = BSL.toStrict (Binary.encode x)

bsToBinary :: Binary v => ByteString -> v
bsToBinary x = Binary.decode (BSL.fromStrict x)

-- | Marshal a 'FilePath' (Haskell string) into a `NUL` terminated C string using
-- temporary storage.
-- On Linux, UTF-8 is almost always the encoding used.
-- When on Windows, UTF-8 can also be used, although the default for those devices is
-- UTF-16. For a more detailed explanation, please refer to
-- https://msdn.microsoft.com/en-us/library/windows/desktop/dd374081(v=vs.85).aspx.
withFilePath :: FilePath -> (CString -> IO a) -> IO a
withFilePath = GHC.withCString GHC.utf8
