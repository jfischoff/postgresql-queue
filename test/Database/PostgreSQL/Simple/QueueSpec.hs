{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module Database.PostgreSQL.Simple.QueueSpec (spec, main) where
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Control.Monad
import           Data.Aeson
import           Data.Function
import           Data.List
import           Database.PostgreSQL.Simple.Queue
import           Database.PostgreSQL.Simple.Queue.Migrate
import           Test.Hspec                     (Spec, hspec, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.DB
import           Control.Monad.Catch
import           Data.List.Split


main :: IO ()
main = hspec spec

schemaName :: String
schemaName = "complicated_name"

spec :: Spec
spec = describeDB (migrate schemaName) "Database.Queue" $ do
  itDB "empty locks nothing" $
    (either throwM return =<< (withPayloadDB schemaName 8 return))
      `shouldReturn` Nothing

  itDB "empty gives count 0" $
    getCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB" $ \conn -> do
    runDB conn $ do
      payloadId <- enqueueDB schemaName $ String "Hello"
      getCountDB schemaName `shouldReturn` 1

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId
        pValue `shouldBe` String "Hello"
        )

      -- read committed but still 0. I don't depend on this but I want to see if it
      -- stays like this.
      getCountDB schemaName `shouldReturn` 0

    runDB conn $ getCountDB schemaName `shouldReturn` 0

  it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 10000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newTVarIO []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      lastCount <- either throwM return =<< withPool' (\c -> withPayload schemaName c 0 $ \(Payload {..}) -> do
        atomically $ do
          xs <- readTVar ref
          writeTVar ref $ pValue : xs
          return $ length xs + 1
        )

      when (lastCount < elementCount) next

    -- Fork a hundred threads and enqueue an index
    -- forM_ [0 .. elementCount - 1] $ \i -> forkIO $ void $ withPool' $ \c ->
    --  enqueue schemaName c $ toJSON i

    forM_ (chunksOf (elementCount `div` 40) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
       forM_ xs $ \i -> enqueue schemaName c $ toJSON i


    waitAnyCancel loopThreads
    xs <- atomically $ readTVar ref
    let Just decoded = mapM (decode . encode) xs
    sort decoded `shouldBe` sort expected

  --  threadDelay maxBound
