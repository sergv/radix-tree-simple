----------------------------------------------------------------------------
-- |
-- Module      :  Data.RadixTree.Internal
-- Copyright   :  (c) Sergey Vinokurov 2018
-- License     :  BSD3-style (see LICENSE)
-- Maintainer  :  serg.foo@gmail.com
----------------------------------------------------------------------------

{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.RadixTree.Internal
  ( RadixTree(..)
  , empty
  , null
  , insert
  , insertWith
  , lookup
  , fromList
  , toList
  , toAscList
  , keys
  , keysSet
  , elems
  , mapMaybe
  , union
  , unionWith
  , size
  ) where

import Prelude hiding (lookup, null)

import Control.Arrow (first)
import Control.DeepSeq
import Control.Monad.ST
import Control.Monad.ST.Unsafe

import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as BSS
import qualified Data.ByteString.Short.Internal as BSSI
import qualified Data.Foldable as Foldable
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IM
import qualified Data.List as L
import Data.Maybe (fromMaybe)
import Data.Primitive.ByteArray
import Data.Set (Set)
import qualified Data.Set as S
import Data.Word
import GHC.Generics (Generic)

-- | A tree data structure that efficiently indexes values by string keys.
data RadixTree a
  = RadixNode
      !(Maybe a)
      !(IntMap (RadixTree a)) -- ^ Either has 0 or 2 or more children, never 1.
  | RadixStr
      !(Maybe a)
      {-# UNPACK #-} !ShortByteString -- ^ Non-empty
      !(RadixTree a)
  deriving (Show, Functor, Foldable, Traversable, Generic)

instance NFData a => NFData (RadixTree a)

empty :: RadixTree a
empty = RadixNode Nothing IM.empty

splitShortByteString :: Int -> ShortByteString -> (ShortByteString, ShortByteString, Word8, ShortByteString)
splitShortByteString n (BSSI.SBS source) = runST $ do
  prefix <- newByteArray prefixSize
  copyByteArray prefix 0 source' 0 prefixSize
  ByteArray prefix# <- unsafeFreezeByteArray prefix
  midSuffix         <- unsafeDupableInterleaveST $ do
    midSuffix <- newByteArray midSuffixSize
    copyByteArray midSuffix 0 source' n midSuffixSize
    unsafeFreezeByteArray midSuffix
  suffix            <- unsafeDupableInterleaveST $ do
    suffix <- newByteArray suffixSize
    copyByteArray suffix 0 source' (n + 1) suffixSize
    unsafeFreezeByteArray suffix
  pure (BSSI.SBS prefix#, byteArrayToBSS midSuffix, indexByteArray source' n, byteArrayToBSS suffix)
  where
    source' = ByteArray source
    prefixSize = n
    midSuffixSize = sizeofByteArray source' - prefixSize
    suffixSize = midSuffixSize - 1

{-# INLINE byteArrayToBSS #-}
byteArrayToBSS :: ByteArray -> BSS.ShortByteString
byteArrayToBSS (ByteArray xs) = BSSI.SBS xs

dropShortByteString :: Int -> ShortByteString -> ShortByteString
dropShortByteString 0  src = src
dropShortByteString !n (BSSI.SBS source) = runST $ do
  dest <- newByteArray sz
  copyByteArray dest 0 source' n sz
  byteArrayToBSS <$> unsafeFreezeByteArray dest
  where
    source' = ByteArray source
    !sz = sizeofByteArray source' - n

singletonShortByteString :: Word8 -> ShortByteString
singletonShortByteString !c = runST $ do
  dest <- newByteArray 1
  writeByteArray dest 0 c
  byteArrayToBSS <$> unsafeFreezeByteArray dest

{-# INLINE unsafeHeadeShortByteString #-}
unsafeHeadeShortByteString :: ShortByteString -> Word8
unsafeHeadeShortByteString = (`BSSI.unsafeIndex` 0)

-- consShortByteString :: Word8 -> ShortByteString -> ShortByteString
-- consShortByteString c (BSSI.SBS source) = runST $ do
--   dest <- newByteArray $! sz + 1
--   writeByteArray dest 0 c
--   copyByteArray dest 1 source' 0 sz
--   byteArrayToBSS <$> unsafeFreezeByteArray dest
--   where
--     source' = ByteArray source
--     !sz = sizeofByteArray source'

data Mismatch
  = IsPrefix
  | CommonPrefixThenMismatch
      !ShortByteString -- ^ Prefix of node contents common with the key
      -- !Word            -- ^ Last byte of the key that caused mismatch
      ShortByteString  -- ^ Suffix with the first mismatching byte
      Word8            -- ^ First byte of the suffix that caused mismatch
      ShortByteString  -- ^ Rest of node contents, suffix
  deriving (Show, Generic)

analyseMismatch
  :: ShortByteString -- ^ Key
  -> Int             -- ^ Key offset
  -> ShortByteString -- ^ Node contents
  -> Mismatch
analyseMismatch (BSSI.SBS key) !keyOffset nodeContentsBS@(BSSI.SBS nodeContents) =
  case findMismatch 0 of
    Nothing          -> IsPrefix
    Just mismatchIdx ->
      case splitShortByteString mismatchIdx nodeContentsBS of
        (prefix, midSuffix, mid, suffix) -> CommonPrefixThenMismatch prefix midSuffix mid suffix
  where
    keySize      = sizeofByteArray key'
    keyLeft      = keySize - keyOffset
    contentsSize = sizeofByteArray nodeContents'

    key'          = ByteArray key
    nodeContents' = ByteArray nodeContents

    limit :: Int
    limit = min keyLeft contentsSize

    findMismatch :: Int -> Maybe Int
    findMismatch !i
      | i == limit
      = if i == contentsSize
        then Nothing
        else Just i -- Key ended in the middle of node's packed key.
      | (indexByteArray key' (keyOffset + i) :: Word8) == indexByteArray nodeContents' i
      = findMismatch $ i + 1
      | otherwise
      = Just i

mkRadixNodeFuse :: Maybe a -> IntMap (RadixTree a) -> Maybe (RadixTree a)
mkRadixNodeFuse val children =
  case val of
    Nothing | IM.null children
      -> Nothing
    val'    | [(c, child)] <- IM.toList children
      -> Just $ RadixStr val' (singletonShortByteString $ fromIntegral c) child
    _ -> Just $ RadixNode val children

-- Precondition: input string is non-empty
mkRadixStrFuse :: Maybe a -> ShortByteString -> RadixTree a -> Maybe (RadixTree a)
mkRadixStrFuse val str rest =
  case (val, rest) of
    (val',    RadixStr Nothing str' rest') ->
      Just $ RadixStr val' (str <> str') rest'
    (Nothing, node)
      | null node -> Nothing
    (val', rest') ->
      Just $ RadixStr val' str rest'

mkRadixStr :: ShortByteString -> RadixTree a -> RadixTree a
mkRadixStr str rest
  | BSS.null str = rest
  | otherwise    = RadixStr Nothing str rest

-- | TODO: prove this function correct.
null :: RadixTree a -> Bool
null = \case
  RadixNode Nothing children -> IM.null children
  RadixStr Nothing _ rest    -> null rest
  _                          -> False

insert :: forall a. ShortByteString -> a -> RadixTree a -> RadixTree a
insert = insertWith const

insertWith :: forall a. (a -> a -> a) -> ShortByteString -> a -> RadixTree a -> RadixTree a
insertWith = insert'

{-# INLINE insert' #-}
insert' :: forall a. (a -> a -> a) -> ShortByteString -> a -> RadixTree a -> RadixTree a
insert' f key value = go 0
  where
    len = BSS.length key

    readKey :: Int -> Int
    readKey = fromIntegral . BSSI.unsafeIndex key

    go :: Int -> RadixTree a -> RadixTree a
    go i
      | i < len
      = \case
        RadixNode oldValue children
          | IM.null children ->
            RadixStr oldValue (dropShortByteString i key) $ RadixNode (Just value) IM.empty
          | otherwise ->
            RadixNode oldValue $
            IM.alter (Just . maybe optNode (go i')) c children
          where
            c :: Int
            c = readKey i
            i' = i + 1
            optNode =
              mkRadixStr (dropShortByteString i' key) $ RadixNode (Just value) IM.empty
        RadixStr oldValue packedKey rest ->
          case analyseMismatch key i packedKey of
            IsPrefix ->
              RadixStr oldValue packedKey $ go (i + BSS.length packedKey) rest
            CommonPrefixThenMismatch prefix midSuffix mid suffix ->
              (if BSS.null prefix then id else RadixStr oldValue prefix) $
                if isKeyEnded
                then
                  RadixStr (Just value) midSuffix rest
                else
                  RadixNode (if BSS.null prefix then oldValue else Nothing) $
                  IM.fromList
                    [ ( mid'
                      , mkRadixStr suffix rest
                      )
                    , ( readKey i'
                      , mkRadixStr (dropShortByteString (i' + 1) key) $ RadixNode (Just value) IM.empty
                      )
                    ]
              where
                i'         = i + BSS.length prefix
                isKeyEnded = i' >= len
                mid'       = fromIntegral mid
      | otherwise
      = \case
        RadixNode oldValue children ->
          RadixNode (Just (maybe value (f value) oldValue)) children
        RadixStr oldValue key' rest ->
          RadixStr (Just (maybe value (f value) oldValue)) key' rest

canStripPrefixFromShortByteString
  :: Int -> ShortByteString -> ShortByteString -> Bool
canStripPrefixFromShortByteString bigStart (BSSI.SBS small) (BSSI.SBS big)
  | bigStart + smallSize > bigSize = False
  | otherwise                      = findMismatch 0
  where
    small' = ByteArray small
    big'   = ByteArray big

    smallSize = sizeofByteArray small'
    bigSize   = sizeofByteArray big'

    findMismatch :: Int -> Bool
    findMismatch !i
      | i == smallSize
      = True
      | (indexByteArray small' i :: Word8) == indexByteArray big' (bigStart + i)
      = findMismatch $ i + 1
      | otherwise
      = False

lookup :: forall a. ShortByteString -> RadixTree a -> Maybe a
lookup key = go 0
  where
    len = BSS.length key

    readKey :: Int -> Int
    readKey = fromIntegral . BSSI.unsafeIndex key

    go :: Int -> RadixTree a -> Maybe a
    go !n tree
      | n == len
      = case tree of
        RadixNode val _  -> val
        RadixStr val _ _ -> val
      | otherwise
      = case tree of
      RadixNode _ children      ->
        IM.lookup (readKey n) children >>= go (n + 1)
      RadixStr _ packedKey rest
        | canStripPrefixFromShortByteString n packedKey key
        -> go (n + BSS.length packedKey) rest
        | otherwise
        -> Nothing

fromList :: [(ShortByteString, a)] -> RadixTree a
fromList =
  L.foldl' (\acc (k, v) -> insert' const k v acc) empty

toList :: RadixTree a -> [(ShortByteString, a)]
toList = toAscList

toAscList :: forall a. RadixTree a -> [(ShortByteString, a)]
toAscList = map (first (BSS.toShort . BSL.toStrict . BSB.toLazyByteString)) . go
  where
    go :: RadixTree a -> [(BSB.Builder, a)]
    go = \case
      RadixNode val children ->
        maybe id (\val' ys -> (mempty, val') : ys) val $
        IM.foldMapWithKey (\c child -> map (first (BSB.word8 (fromIntegral c) <>)) $ go child) children
      RadixStr val packedKey rest ->
        maybe id (\val' ys -> (mempty, val') : ys) val $
        map (first (BSB.shortByteString packedKey <>)) $
        go rest

keys :: RadixTree a -> [ShortByteString]
keys = map (BSS.toShort . BSL.toStrict . BSB.toLazyByteString) . go
  where
    go :: RadixTree a -> [BSB.Builder]
    go = \case
      RadixNode val children ->
        maybe id (\_ ys -> mempty : ys) val $
        IM.foldMapWithKey (\c child -> map (BSB.word8 (fromIntegral c) <>) $ go child) children
      RadixStr val packedKey rest ->
        maybe id (\_ ys -> mempty : ys) val $
        map (BSB.shortByteString packedKey <>) $
        go rest

keysSet :: RadixTree a -> Set ShortByteString
keysSet = S.fromDistinctAscList . keys

elems :: RadixTree a -> [a]
elems = Foldable.toList

mapMaybe :: forall a b. (a -> Maybe b) -> RadixTree a -> RadixTree b
mapMaybe f = fromMaybe empty . go
  where
    go :: RadixTree a -> Maybe (RadixTree b)
    go = \case
      RadixNode val children ->
        mkRadixNodeFuse (f =<< val) $ IM.mapMaybe go children
      RadixStr val str rest ->
        mkRadixStrFuse (f =<< val) str $ fromMaybe empty $ go rest

-- | O(n + m) Combine two radix trees trees. If a key is present in both
-- trees then the value from left one will be retained.
union :: RadixTree a -> RadixTree a -> RadixTree a
union = unionWith const

-- | O(n + m) Combine two trees using supplied function to resolve
-- values that have the same key in both trees.
unionWith :: forall a. (a -> a -> a) -> RadixTree a -> RadixTree a -> RadixTree a
unionWith f = go
  where
    combineVals :: Maybe a -> Maybe a -> Maybe a
    combineVals x y = case (x, y) of
      (Nothing,   Nothing)   -> Nothing
      (Nothing,   y'@Just{}) -> y'
      (x'@Just{}, Nothing)   -> x'
      (Just x',   Just y')   -> Just $ f x' y'

    go :: RadixTree a -> RadixTree a -> RadixTree a
    go x y = case (x, y) of
      (RadixNode val children, RadixNode val' children') ->
        RadixNode (combineVals val val') (IM.unionWith go children children')
      (RadixNode val children, RadixStr val' str' rest') ->
        RadixNode (combineVals val val') $
          (\g -> IM.alter g h children) $ \child ->
            Just $!
            let rest'' = mkRadixStr (dropShortByteString 1 str') rest' in
            case child of
              Nothing     -> rest''
              Just child' -> go child' rest''
        where
          h = fromIntegral $ unsafeHeadeShortByteString str'
      (RadixStr val str rest, RadixNode val' children') ->
        RadixNode (combineVals val val') $
          (\g -> IM.alter g h children') $ \child ->
            Just $!
            let rest' = mkRadixStr (dropShortByteString 1 str) rest in
            case child of
              Nothing     -> rest'
              Just child' -> go rest' child'
        where
          h = fromIntegral $ unsafeHeadeShortByteString str
      (RadixStr val str rest, RadixStr val' str' rest') ->
        case analyseMismatch str 0 str' of
          -- str' is a prefix of str
          IsPrefix ->
            RadixStr (combineVals val val') str' $
              go (mkRadixStr (dropShortByteString (BSS.length str') str) rest) rest'
          -- str' = prefix + firstMismatchStr' + suffixStr'
          --      = prefix + midSuffixStr'
          CommonPrefixThenMismatch prefix midSuffixStr' firstMismatchStr' suffixStr' ->
            (if BSS.null prefix then id else RadixStr (combineVals val val') prefix) $
              if BSS.length prefix == BSS.length str
              then
                go rest $ RadixStr
                  (if BSS.null prefix then combineVals val val' else Nothing)
                  midSuffixStr'
                  rest'
              else RadixNode (if BSS.null prefix then combineVals val val' else Nothing) $ IM.fromList
                [ ( fromIntegral firstMismatchStr'
                  , mkRadixStr suffixStr' rest'
                  )
                , ( fromIntegral $ BSSI.unsafeIndex str $ BSS.length prefix
                  , mkRadixStr (dropShortByteString (BSSI.length prefix + 1) str) rest
                  )
                ]

-- | O(n) Get number of elements in a radix tree
size :: RadixTree a -> Int
size = length
