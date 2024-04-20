{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}

{-# LANGUAGE ImportQualifiedPost #-}

{-# OPTIONS_GHC -Wno-x-partial #-}

module Main (main) where

import Control.Arrow
import Control.DeepSeq
import Control.Exception
import Data.Coerce
import Data.Foldable
import Data.Hashable
import Data.List qualified as L

import Data.ByteString.Short qualified as BSS
import Data.HashMap.Strict qualified as HM
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as M
import Data.Text.Encoding qualified as TE
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.IO qualified as TLIO

import Data.HashTable.IO qualified as HT

import Test.Tasty.Bench

import Data.RadixTree.Internal qualified as RT

-- import Data.Patricia.Word.Strict qualified as Patricia.Strict
-- import Data.Zebra.Word qualified as Zebra
import Data.Radix1Tree.Word8.Strict qualified as Radix1Tree.Strict
import Data.Radix1Tree.Word8.Key.Unsafe qualified as Radix1Tree.Strict

newtype ShortByteStringFastOrd = ShortByteStringFastOrd BSS.ShortByteString
  deriving (Eq, Hashable, NFData)

instance Ord ShortByteStringFastOrd where
  ShortByteStringFastOrd x `compare` ShortByteStringFastOrd y =
    compare (BSS.length x) (BSS.length y) <> compare x y

main :: IO ()
main = do
  contents <- TLIO.readFile "tags-ebac8dcc87fd1f1b1e7016d6585549309e3c5016-haskell-mode"
  let tags :: [TL.Text]
      tags = filter (not . TL.null) $ map (head . TL.splitOn "\t") $ drop 1 $ TL.lines contents

      decodeBS = TE.encodeUtf8 . TL.toStrict
      decode = BSS.toShort . decodeBS

      tags' :: [BSS.ShortByteString]
      tags' = map decode tags

      tags'' :: [(BSS.ShortByteString, ())]
      tags'' = map (id &&& const ()) tags'

      tagsRev'' :: [(BSS.ShortByteString, ())]
      tagsRev'' = map ((BSS.pack . reverse . BSS.unpack) &&& const ()) tags'

      tagsFastOrd'' :: [(ShortByteStringFastOrd, ())]
      tagsFastOrd'' = coerce tags''

      tagsRevFastOrd'' :: [(ShortByteStringFastOrd, ())]
      tagsRevFastOrd'' = coerce tagsRev''


      -- tagsBS :: [(BS.ByteString, ())]
      -- tagsBS = map (decodeBS &&& const ()) tags

      queriesPresent :: [BSS.ShortByteString]
      queriesPresent = tags' ++ map (BSS.pack . reverse . BSS.unpack) tags'

      queriesMissing :: [BSS.ShortByteString]
      queriesMissing = map (BSS.pack . reverse . BSS.unpack) tags'

      queriesBoth :: [BSS.ShortByteString]
      queriesBoth = tags' ++ map (BSS.pack . reverse . BSS.unpack) tags'

  evaluate $ rnf tags'
  evaluate $ rnf tags''
  evaluate $ rnf tagsRev''
  evaluate $ rnf tagsFastOrd''
  evaluate $ rnf tagsRevFastOrd''
  evaluate $ rnf queriesPresent
  evaluate $ rnf queriesMissing
  evaluate $ rnf queriesBoth

  let radixTree         = RT.fromList tags''
      radixTreeRev      = RT.fromList tagsRev''
      treeMap           = M.fromList  tags''
      treeMapRev        = M.fromList  tagsRev''
      treeMapFastOrd    :: Map ShortByteStringFastOrd ()
      treeMapFastOrd    = M.fromList  tagsFastOrd''
      treeMapRevFastOrd :: Map ShortByteStringFastOrd ()
      treeMapRevFastOrd = M.fromList  tagsRevFastOrd''
      hashMap           = HM.fromList tags''
      hashMapRev        = HM.fromList tagsRev''

      radixNewTree      = L.foldl' (\acc (k, v) -> Radix1Tree.Strict.insert (Radix1Tree.Strict.unsafeFeedShortByteString k) v acc) Radix1Tree.Strict.empty tags''
      radixNewTreeRev   = L.foldl' (\acc (k, v) -> Radix1Tree.Strict.insert (Radix1Tree.Strict.unsafeFeedShortByteString k) v acc) Radix1Tree.Strict.empty tagsRev''

  evaluate $ rnf radixTree
  evaluate $ rnf radixTreeRev
  evaluate $ rnf treeMap
  evaluate $ rnf treeMapRev
  evaluate $ rnf treeMapFastOrd
  evaluate $ rnf treeMapRevFastOrd
  evaluate $ rnf hashMap
  evaluate $ rnf hashMapRev

  evaluate $ rnf radixNewTree
  evaluate $ rnf radixNewTreeRev

  (basic  :: HT.BasicHashTable  BSS.ShortByteString ()) <- HT.new
  -- (linear :: HT.LinearHashTable BSS.ShortByteString ()) <- HT.new
  (cuckoo :: HT.CuckooHashTable BSS.ShortByteString ()) <- HT.new
  for_ tags'' $ \(k, v) -> do
    HT.insert basic  k v
    -- HT.insert linear k v
    HT.insert cuckoo k v

  defaultMain
    [ bgroup "creation"
      [ bench "Data.RadixTree"  $ nf RT.fromList tags''
      , bench "Data.Map"        $ nf M.fromList tags''
      , bench "Data.Map fast ord" $ nf M.fromList tagsFastOrd''
      , bench "Data.HashMap"    $ nf HM.fromList tags''
      , bench "BasicHashTable"  $ nfIO $ do
          (ht :: HT.BasicHashTable  BSS.ShortByteString ()) <- HT.new
          for_ tags'' $ \(k, v) -> HT.insert ht k v
      -- , bench "LinearHashTable"  $ nfIO $ do
      --     (ht :: HT.LinearHashTable BSS.ShortByteString ()) <- HT.new
      --     for_ tags'' $ \(k, v) -> HT.insert ht k v
      , bench "CuckooHashTable"  $ nfIO $ do
          (ht :: HT.CuckooHashTable BSS.ShortByteString ()) <- HT.new
          for_ tags'' $ \(k, v) -> HT.insert ht k v
      , bench "Data.Radix1Tree.Word8.Strict" $
        nf
          (L.foldl' (\acc (k, v) -> Radix1Tree.Strict.insert (Radix1Tree.Strict.unsafeFeedShortByteString k) v acc) Radix1Tree.Strict.empty)
          tags''
      ]
    , bgroup "lookup"
        [ bgroup "present"
          [ bench "Data.RadixTree"               $ nf (map (`RT.lookup` radixTree))      queriesPresent
          , bench "Data.Map"                     $ nf (map (`M.lookup`  treeMap))        queriesPresent
          , bench "Data.Map fast ord"            $ nf (map (`M.lookup`  treeMapFastOrd)) (coerce queriesPresent)
          , bench "Data.HashMap"                 $ nf (map (`HM.lookup` hashMap))        queriesPresent
          , bench "BasicHashTable"               $ nfIO $ traverse (HT.lookup basic)     queriesPresent
          -- , bench "LinearHashTable"           $ nfIO $ traverse (HT.lookup linear)    queriesPresent
          , bench "CuckooHashTable"              $ nfIO $ traverse (HT.lookup cuckoo)    queriesPresent
          , bench "Data.Radix1Tree.Word8.Strict" $ nf (map ((`Radix1Tree.Strict.lookup` radixNewTree) . Radix1Tree.Strict.unsafeFeedShortByteString)) queriesPresent
          ]
        , bgroup "missing"
          [ bench "Data.RadixTree"               $ nf (map (`RT.lookup` radixTree))      queriesMissing
          , bench "Data.Map"                     $ nf (map (`M.lookup`  treeMap))        queriesMissing
          , bench "Data.Map fast ord"            $ nf (map (`M.lookup`  treeMapFastOrd)) (coerce queriesMissing)
          , bench "Data.HashMap"                 $ nf (map (`HM.lookup` hashMap))        queriesMissing
          , bench "BasicHashTable"               $ nfIO $ traverse (HT.lookup basic)     queriesMissing
          -- , bench "LinearHashTable"           $ nfIO $ traverse (HT.lookup linear)    queriesMissing
          , bench "CuckooHashTable"              $ nfIO $ traverse (HT.lookup cuckoo)    queriesMissing
          , bench "Data.Radix1Tree.Word8.Strict" $ nf (map ((`Radix1Tree.Strict.lookup` radixNewTree) . Radix1Tree.Strict.unsafeFeedShortByteString)) queriesMissing
          ]
        , bgroup "both"
          [ bench "Data.RadixTree"               $ nf (map (`RT.lookup` radixTree))      queriesBoth
          , bench "Data.Map"                     $ nf (map (`M.lookup`  treeMap))        queriesBoth
          , bench "Data.Map fast ord"            $ nf (map (`M.lookup`  treeMapFastOrd)) (coerce queriesBoth)
          , bench "Data.HashMap"                 $ nf (map (`HM.lookup` hashMap))        queriesBoth
          , bench "BasicHashTable"               $ nfIO $ traverse (HT.lookup basic)     queriesBoth
          -- , bench "LinearHashTable"           $ nfIO $ traverse (HT.lookup linear)    queriesBoth
          , bench "CuckooHashTable"              $ nfIO $ traverse (HT.lookup cuckoo)    queriesBoth
          , bench "Data.Radix1Tree.Word8.Strict" $ nf (map ((`Radix1Tree.Strict.lookup` radixNewTree) . Radix1Tree.Strict.unsafeFeedShortByteString)) queriesBoth
          ]
        ]
    , bgroup "keys"
      [ bench "Data.RadixTree" $ nf RT.keys radixTree
      , bench "Data.Map"       $ nf M.keys treeMap
      , bench "Data.HashMap"   $ nf HM.keys hashMap
      ]
    , bgroup "toList"
      [ bench "Data.RadixTree" $ nf RT.toList radixTree
      , bench "Data.Map"       $ nf M.toList treeMap
      , bench "Data.HashMap"   $ nf HM.toList hashMap
      ]
    , bgroup "union"
      [ bench "Data.RadixTree"    $ nf (uncurry RT.union) (radixTree, radixTreeRev)
      , bench "Data.Map"          $ nf (uncurry M.union) (treeMap, treeMapRev)
      , bench "Data.Map fast ord" $ nf (uncurry M.union) (treeMapFastOrd, treeMapRevFastOrd)
      , bench "Data.HashMap"      $ nf (uncurry HM.union) (hashMap, hashMapRev)
      , bench "Data.Radix1Tree.Word8.Strict" $ nf (uncurry Radix1Tree.Strict.union) (radixNewTree, radixNewTreeRev)
      ]
    ]
