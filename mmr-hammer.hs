{-# LANGUAGE ViewPatterns, CPP, ScopedTypeVariables #-}

import Prelude hiding (catch)

import qualified Data.Set as S
import Data.Char
import Data.Maybe
import Data.List
import Data.IORef

import Control.Monad
import Control.Exception

import System.IO
import System.IO.Unsafe
import System.Exit
import System.Environment
import System.Console.GetOpt
import System.Directory
import System.FilePath

import Text.Printf

import LDAP.Init (ldapInitialize, ldapSimpleBind)
import LDAP.Constants (ldapPort)
import LDAP.Search (SearchAttributes(LDAPAllUserAttrs), LDAPEntry(..),
    LDAPScope(..), ldapSearch)
import LDAP.Modify (LDAPModOp(..), LDAPMod(..), ldapAdd, ldapDelete,
    ldapModify, list2ldm)

scriptsBase = "dc=scripts,dc=mit,dc=edu"
configBase  = "cn=config"
replicaBase = "cn=replica,cn=\"dc=scripts,dc=mit,dc=edu\",cn=mapping tree,cn=config"

searchScripts = search scriptsBase
searchConfig  = search configBase
searchReplica = search replicaBase
getEntry ldap dn = debugIOVal ("getEntry: " ++ dn) $
    listToMaybe `fmap` ldapSearch ldap (Just dn) LdapScopeBase Nothing LDAPAllUserAttrs False
search base ldap query = debugIOVal ("search: " ++ query ++ " -b " ++ base) $
    ldapSearch ldap (Just base) LdapScopeSubtree (Just query) LDAPAllUserAttrs False

normalizeKey    = map toLower
lookupKey (normalizeKey -> key) attrs = maybe [] id $ lookup key (map (\(normalizeKey -> k,v)->(k,v)) attrs)
lookupKey1 key = listToMaybe . lookupKey key
constructKeySet = S.fromList . map normalizeKey

-- mungeAgreements should live in some error monad; right now using IO
getAgreements ldap = mapM mungeAgreement =<< getRawAgreements ldap
getRawAgreements ldap = searchReplica ldap "objectClass=nsDS5ReplicationAgreement"
mungeAgreement (LDAPEntry dn attrs) = do
    attrs' <- filterM replicaConfigPredicate attrs
    return (LDAPEntry dn attrs')
replicaConfigPredicate (normalizeKey -> name, _)
    | S.member name replicaConfig  = return True
    | S.member name replicaRuntime = return False
    | otherwise = error ("replicaConfigPredicate: Unrecognized replica key " ++ name)

getConfig ldap = do
    r <- getEntry ldap configBase
    case r of
        Nothing -> error "getConfig: No config object found"
        Just x -> return x
getVersion ldap = do
    (LDAPEntry _ attrs) <- getConfig ldap
    case lookupKey1 "nsslapd-versionstring" attrs of
        Nothing -> error "getVersion: No version field in config found"
        Just x -> return x
getReplica ldap = do
    r <- getEntry ldap replicaBase
    case r of
        Nothing -> error "getReplica: No replica object found"
        Just x -> return x
getBinds ldap = do
    (LDAPEntry _ attrs) <- getReplica ldap
    case lookupKey "nsDS5ReplicaBindDN" attrs of
        [] -> error "getBinds: No binds found"
        bs -> return bs

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
        error "suspendAgreements: Refusing to write empty replicas file"
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
        error "suspendBinds: Refusing to write empty binds file"
    withFile statefile WriteMode $ \h ->
        hPutStr h (show binds)
    ldapModify ldap replicaBase [LDAPMod LdapModDelete "nsDS5ReplicaBindDN" []]

restoreBinds ldap statefile = do
    binds <- fmap read (readFile statefile)
    ldapModify ldap replicaBase [LDAPMod LdapModAdd "nsDS5ReplicaBindDN" binds]

createLdap uri user password = do
    ldap <- ldapInitialize uri
    -- XXX LDAP has no support for other bind methods (yet)
    ldapSimpleBind ldap user password
    return ldap

printStatus ldap = do
    rawAgreements <- getRawAgreements ldap
    let width = maximum (0:concatMap (map length . lookupKey "nsDS5ReplicaHost" . leattrs) rawAgreements)
    forM_ rawAgreements $ \(LDAPEntry dn attrs) -> do
        let mhost   = lookupKey1 "nsDS5ReplicaHost" attrs
            mstatus = lookupKey1 "nsds5replicaLastUpdateStatus" attrs
        case (mhost, mstatus) of
            (Just host, Just status) ->
                printf ("%-" ++ show width ++ "s : %s\n") host status
            otherwise -> warnIO ("Malformed replication agreement at " ++ dn)

printVersion ldap = getVersion ldap >>= putStrLn

data Password = Password String | AskPassword | NoPassword

data Options = Options {
      optUri      :: Maybe String
    , optPassword :: Password
    , optUser     :: Maybe String
    , optDebug    :: Bool
}

defaultOptions = Options {
      optUri        = Nothing
    , optPassword   = NoPassword
    , optUser       = Nothing
    , optDebug      = False
}

putOptHost h r = r { optUri = Just ("ldap://" ++ h) }
putOptPassword p r = r {
    optPassword = case p of
        Just p  -> Password p
        Nothing -> AskPassword
    }

#define PUT(field) (\x r -> r {field = x})
#define PUTX(field, x) (\r -> r {field = x})
#define PUTJ(field) (\x r -> r {field = Just x})
options =
    [ Option []    ["uri"]      (ReqArg PUTJ(optUri) "URI")      "URI of LDAP server"
    , Option ['h'] ["host"]     (ReqArg putOptHost "HOST")       "host, connect with ldap schema"
    , Option ['p'] ["password"] (OptArg putOptPassword "PASS")   "password"
    , Option ['u'] ["user"]     (ReqArg PUTJ(optUser) "USER")    "dn of user to bind as"
    , Option ['d'] ["debug"]    (NoArg  PUTX(optDebug, True))    "debugging output"
    ]

parseOptions = do
    argv <- getArgs
    case getOpt Permute options argv of
        (optlist, args@(_:_), []) ->
            return (foldl (flip ($)) defaultOptions optlist, args)
        (_,_,errs) -> do
            hPutStr stderr (concat errs ++ usageInfo header options)
            exitFailure
    where header = "Usage: mmr-hammer [print|suspend|restore] [binds|agreements]\n"

fillWith opts mUri mUser mPassword= do
    uri <- case optUri opts of
        Just uri -> return uri
        Nothing -> mUri
    user <- case optUser opts of
        Just user -> return user
        Nothing -> mUser
    password <- case optPassword opts of
        Password p -> return p
        AskPassword -> askPassword
        NoPassword -> mPassword
    return (uri, user, password)

defaultStrategy opts = do
    debugIO "Trying defaultStrategy"
    fillWith opts (error "Missing host") (error "Missing user") (error "Missing password")
ldapVircStrategy opts = do
    debugIO "Trying ldapVircStrategy"
    -- Only supports "default" profile for now
    home <- getHomeDirectory
    contents <- lines `fmap` readFile (home </> ".ldapvirc")
    let parseLdapVirc = do
        let section = takeWhile (not . isPrefixOf "profile") . tail
                    . dropWhile (/= "profile default") $ contents
            getField name = let prefix = name ++ " "
                            in evaluate . fromJust . stripPrefix prefix . fromJust . find (isPrefixOf prefix) $ section
        fillWith opts (getField "host") (getField "user") (getField "password")
    bracketOnError (return ())
                   (const (warnIO "Failed to parse .ldapvirc"))
                   (const parseLdapVirc)
fallbackStrategy opts = do
    debugIO "Using fallbackStrategy"
    let mUri = do
            warnIO "Defaulting to ldap://localhost (try --uri or --host)"
            return "ldap://localhost"
        mUser = do
            warnIO "Defaulting to cn=Directory Manager (try --user)"
            return "cn=Directory Manager"
        mPassword = do
            warnIO "Defaulting to empty password (try --password)"
            return "" -- XXX semantics not quite right
    fillWith opts mUri mUser mPassword

askPassword = bracket (hSetEcho stdin False)
                      (const $ hSetEcho stdin True >> hPutStr stderr "\n")
                      (const $ hPutStr stderr "Password: " >>
                               hFlush stderr >>
                               hGetLine stdin)

tryAll [] = error "tryAll: empty list, please supply fallback"
tryAll [x] = x
tryAll (x:xs) = catch x (\(e :: SomeException) -> debugIO ("tryAll: Failed with " ++ show e) >> tryAll xs)

isDebugging = unsafePerformIO (newIORef False)
{-# NOINLINE isDebugging #-}

debugIO msg = do
    b <- readIORef isDebugging
    when b (hPutStrLn stderr $ "DEBUG: " ++ msg)
debugIOVal msg m = do
    debugIO msg
    m
warnIO msg = hPutStrLn stderr $ "WARNING: " ++ msg

main = do
    let replicasFile = "mmr-hammer-replica"
        bindsFile    = "mmr-hammer-binds"
    (opts, args) <- parseOptions
    when (optDebug opts) (writeIORef isDebugging True)
    (uri, user, password) <- tryAll $ [defaultStrategy, ldapVircStrategy, fallbackStrategy] `ap` [opts]
    ldap <- createLdap uri user password
    case args of
        ["print",   "binds"] -> printBinds ldap
        ["suspend", "binds"] -> suspendBinds ldap bindsFile
        ["restore", "binds"] -> restoreBinds ldap bindsFile
        ["print",   "agreements"] -> printAgreements ldap
        ["suspend", "agreements"] -> suspendAgreements ldap replicasFile
        ["restore", "agreements"] -> restoreAgreements ldap replicasFile
        ["status"] -> printStatus ldap
        ["version"] -> printVersion ldap
        [] -> putStrLn "mmr-hammer.hs [print|suspend|restore] [binds|agreements]"
        _ -> error "Unknown argument"
