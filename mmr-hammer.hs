{-# LANGUAGE ViewPatterns #-}

import qualified Data.Set as S
import Data.Char

import Control.Monad

import System.IO
import System.Environment

import LDAP.Init (ldapInit, ldapSimpleBind)
import LDAP.Constants (ldapPort)
import LDAP.Search (SearchAttributes(LDAPAllUserAttrs), LDAPEntry(..),
    LDAPScope(LdapScopeSubtree), ldapSearch)
import LDAP.Modify (LDAPModOp(..), LDAPMod(..), ldapAdd, ldapDelete,
    ldapModify, list2ldm)

scriptsBase = "dc=scripts,dc=mit,dc=edu"
configBase  = "cn=config"
replicaBase = "cn=replica,cn=\"dc=scripts,dc=mit,dc=edu\",cn=mapping tree,cn=config"

searchScripts = search scriptsBase
searchConfig  = search configBase
searchReplica = search replicaBase
search base ldap query = ldapSearch ldap (Just base) LdapScopeSubtree (Just query) LDAPAllUserAttrs False

normalizeKey    = map toLower
lookupKey (normalizeKey -> key) attrs = lookup key (map (\(normalizeKey -> k,v)->(k,v)) attrs)
constructKeySet = S.fromList . map normalizeKey

-- mungeAgreements should live in some error monad; right now using IO
getAgreements ldap = mapM mungeAgreement =<< searchReplica ldap "objectClass=nsDS5ReplicationAgreement"
mungeAgreement (LDAPEntry dn attrs) = do
    attrs' <- filterM replicaConfigPredicate attrs
    return (LDAPEntry dn attrs')
replicaConfigPredicate (normalizeKey -> name, _)
    | S.member name replicaConfig  = return True
    | S.member name replicaRuntime = return False
    | otherwise = error ("Unrecognized replica key " ++ name)

getReplica ldap = do
    r <- searchReplica ldap "objectClass=nsDS5Replica"
    case r of
        [] -> error "No replica object found"
        [x] -> return x
        otherwise -> error "Too many replica objects found"
getBinds ldap = do
    (LDAPEntry _ attrs) <- getReplica ldap
    case lookupKey "nsDS5ReplicaBindDN" attrs of
        Nothing -> error "No binds found"
        Just bs -> return bs

-- what goes in when you create a replication agreement
replicaConfig = constructKeySet
    [ "objectClass"
    , "cn"
    , "nsDS5ReplicaHost"
    , "nsDS5ReplicaRoot"
    , "nsDS5ReplicaPort"
    , "nsDS5ReplicaTransportInfo"
    , "nsDS5ReplicaBindDN"
    , "nsDS5ReplicaBindMethod"
    , "nsDS5ReplicaUpdateSchedule"
    , "nsds5replicaTimeout"
    ]
-- what fedora ds generates at runtime
replicaRuntime = constructKeySet
    [ "creatorsName"
    , "modifiersName"
    , "createTimestamp"
    , "modifyTimestamp"
    , "nsds5replicareapactive"
    , "nsds5replicaLastUpdateStart"
    , "nsds5replicaLastUpdateEnd"
    , "nsds5replicaChangesSentSinceStartup"
    , "nsds5replicaLastUpdateStatus"
    , "nsds5replicaUpdateInProgress"
    , "nsds5replicaLastInitStart"
    , "nsds5replicaLastInitEnd"
    , "nsds5replicaLastInitStatus"
    , "nsds50ruv"
    , "nsruvReplicaLastModified"
    ]

ldapAddEntry ldap (LDAPEntry dn attrs) =
    ldapAdd ldap dn (list2ldm LdapModAdd attrs)

ldapDeleteEntry ldap (LDAPEntry dn _ ) =
    ldapDelete ldap dn

printAgreements ldap = do
    replicas <- getAgreements ldap
    putStrLn (entriesToLdif replicas)

suspendAgreements ldap statefile = do
    replicas <- getAgreements ldap
    when (null replicas) $
        error "Refusing to write empty replicas file"
    withFile statefile WriteMode $ \h ->
        hPutStr h (serializeEntries replicas)
    withFile (statefile ++ ".ldif") WriteMode $ \h ->
        hPutStr h (entriesToLdif replicas)
    mapM_ (ldapDeleteEntry ldap) replicas

restoreAgreements ldap statefile = do
    replicas <- fmap unserializeEntries (readFile statefile)
    mapM_ (ldapAddEntry ldap) replicas

serializeEntries = show . map (\(LDAPEntry dn attrs) -> (dn, attrs))
unserializeEntries = map (\(dn, attrs) -> LDAPEntry dn attrs) . read

entriesToLdif = unlines . map entryToLdif
entryToLdif (LDAPEntry dn attrs) = "add " ++ dn ++ "\n" ++ concatMap renderAttr attrs
    where renderAttr (k, vs) = concatMap (\v -> k ++ ": " ++ v ++ "\n") vs

printBinds ldap = do
    binds <- getBinds ldap
    mapM_ putStrLn binds

suspendBinds ldap statefile = do
    binds <- getBinds ldap
    when (null binds) $
        error "Refusing to write empty binds file"
    withFile statefile WriteMode $ \h ->
        hPutStr h (show binds)
    ldapModify ldap replicaBase [LDAPMod LdapModDelete "nsDS5ReplicaBindDN" []]

restoreBinds ldap statefile = do
    binds <- fmap read (readFile statefile)
    ldapModify ldap replicaBase [LDAPMod LdapModAdd "nsDS5ReplicaBindDN" binds]

createLdap = do
    -- XXX change this into some legit configuration
    let server = "localhost"
    password <- readFile "/etc/signup-ldap-pw"
    ldap <- ldapInit server ldapPort
    ldapSimpleBind ldap "cn=Directory Manager" password
    return ldap

main = do
    let replicasFile = "mmr-hammer-replica"
        bindsFile    = "mmr-hammer-binds"
    args <- getArgs
    ldap <- createLdap
    case args of
        ["print",   "binds"] -> printBinds ldap
        ["suspend", "binds"] -> suspendBinds ldap bindsFile
        ["restore", "binds"] -> restoreBinds ldap bindsFile
        ["print",   "agreements"] -> printAgreements ldap
        ["suspend", "agreements"] -> suspendAgreements ldap replicasFile
        ["restore", "agreements"] -> restoreAgreements ldap replicasFile
        [] -> putStrLn "mmr-hammer.hs [print|suspend|restore] [binds|agreements]"
        _ -> error "Unknown argument"
