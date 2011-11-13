{-# LANGUAGE ViewPatterns, CPP, ScopedTypeVariables #-}

import Prelude hiding (catch)

import qualified Data.Set as S
import Data.Char
import Data.Maybe
import Data.List
import Data.IORef
import Data.Time

import Control.Monad
import Control.Exception
import Control.Concurrent
import Control.Applicative

import System.IO
import System.IO.Unsafe
import System.Exit
import System.Environment
import System.Console.GetOpt
import System.Directory
import System.FilePath
import System.Locale

import Text.Printf

import Network.URI
import Network.BSD

import LDAP.Init (ldapInitialize, ldapSimpleBind)
import LDAP.Search (SearchAttributes(LDAPAllUserAttrs), LDAPEntry(..),
    LDAPScope(..), ldapSearch)
import LDAP.Modify (LDAPModOp(..), LDAPMod(..), ldapAdd, ldapDelete,
    ldapModify, list2ldm)
import LDAP.Data (LDAPReturnCode(..))
import LDAP.Exceptions (LDAPException(..), catchLDAP, throwLDAP)

-------------------------------------------------------------------------------
-- Type safety

newtype Canonical = Canonical { canonical :: HostName }
canonicalize h = Canonical . map toLower . hostName <$> getHostByName h

-------------------------------------------------------------------------------
-- Metadata

scriptsBase = "dc=scripts,dc=mit,dc=edu"
configBase  = "cn=config"
replicaBase = "cn=replica,cn=\"dc=scripts,dc=mit,dc=edu\",cn=mapping tree,cn=config"
agreementCn target = "GSSAPI Replication to " ++ target
agreementDn cn = "cn=\"" ++ cn ++ "\"," ++ replicaBase
testDn = "scriptsVhostName=replication-test,ou=VirtualHosts," ++ scriptsBase

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
    , "nsds5beginreplicarefresh"
    ]

-------------------------------------------------------------------------------
-- Utility functions

searchScripts = search scriptsBase
searchConfig  = search configBase
searchReplica = search replicaBase
search base ldap querystr = debugIOVal ("search: " ++ querystr ++ " -b " ++ base) $
    ldapSearch ldap (Just base) LdapScopeSubtree (Just querystr) LDAPAllUserAttrs False

getEntry ldap dn = debugIOVal ("getEntry: " ++ dn) $
    listToMaybe `fmap` ldapSearch ldap (Just dn) LdapScopeBase Nothing LDAPAllUserAttrs False

normalizeKey    = map toLower
lookupKey (normalizeKey -> key) attrs = maybe [] id $ lookup key (map (\(normalizeKey -> k,v)->(k,v)) attrs)
lookupKey1 key = listToMaybe . lookupKey key
constructKeySet = S.fromList . map normalizeKey

ldapAddEntry ldap (LDAPEntry dn attrs) =
    ldapAdd ldap dn (list2ldm LdapModAdd attrs)
ldapDeleteEntry ldap (LDAPEntry dn _ ) =
    ldapDelete ldap dn

createLdap uri user password = do
    debugIO $ "createLdap: Connecting to " ++ uri
    ldap <- ldapInitialize uri
    -- XXX LDAP has no support for other bind methods (yet)
    ldapSimpleBind ldap user password
    return ldap

-------------------------------------------------------------------------------
-- Data representation

serializeEntries = show . map (\(LDAPEntry dn attrs) -> (dn, attrs))
unserializeEntries = map (\(dn, attrs) -> LDAPEntry dn attrs) . read

entriesToLdif = unlines . map entryToLdif
entryToLdif (LDAPEntry dn attrs) = "add " ++ dn ++ "\n" ++ concatMap renderAttr attrs
    where renderAttr (k, vs) = concatMap (\v -> k ++ ": " ++ v ++ "\n") vs

-------------------------------------------------------------------------------
-- Managing replication agreements

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

-------------------------------------------------------------------------------
-- Queries

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
getConflicts ldap = searchScripts ldap "nsds5ReplConflict=*"
getLocalhost ldap = do
    (LDAPEntry _ attrs) <- getConfig ldap
    case lookupKey1 "nsslapd-localhost" attrs of
        Nothing -> error "getLocalhost: No localhost name in config found"
        Just x -> return x

-------------------------------------------------------------------------------
-- Commands

printAgreements ldap = do
    replicas <- getAgreements ldap
    putStrLn (entriesToLdif replicas)

suspendAgreements ldap statefile = do
    replicas <- getAgreements ldap
    when (null replicas) $
        error "suspendAgreements: Cowardly refusing to write empty replicas file"
    withFile statefile WriteMode $ \h ->
        hPutStr h (serializeEntries replicas)
    withFile (statefile ++ ".ldif") WriteMode $ \h ->
        hPutStr h (entriesToLdif replicas)
    mapM_ (ldapDeleteEntry ldap) replicas

restoreAgreements ldap statefile = do
    replicas <- fmap unserializeEntries (readFile statefile)
    mapM_ (ldapAddEntry ldap) replicas

-- Hack; you should probably find agreements using something similar to
-- forEachRawAgreement
removeRedundantReplication ldap = do
    master <- getLocalhost ldap
    ldapDelete ldap (agreementDn (agreementCn master))

reinitAgreements ldap statefile = do
    putStrLn "Disabling replication"
    disableReplication ldap
    putStrLn "Suspending agreements"
    suspendAgreements ldap statefile
    putStrLn "Restoring agreements"
    restoreAgreements ldap statefile
    putStrLn "Enabling replication"
    enableReplication ldap
    putStrLn "Done!"

initAgreements ldap targets = do
    master <- getLocalhost ldap
    forM_ targets $ \target -> do
        host <- canonicalize target
        if canonical host /= master
            then do
                initAgreement ldap master host `catchLDAP` \e ->
                    if code e == LdapAlreadyExists
                        then putStrLn ("Agreement already exists for " ++ canonical host)
                        else throwLDAP e
            else putStrLn ("Cowardly refusing to replicate with self")

initAgreement ldap master (Canonical target) = do
    putStrLn ("Initializing agreement to " ++ target)
    let cn = agreementCn target
    ldapAdd ldap (agreementDn cn) $ list2ldm LdapModAdd
        [ ("objectClass", ["top", "nsDS5ReplicationAgreement"])
        , ("cn", [cn])
        , ("nsDS5ReplicaHost", [target])
        , ("nsDS5ReplicaRoot", ["dc=scripts,dc=mit,dc=edu"])
        , ("nsDS5ReplicaPort", ["389"])
        , ("nsDS5ReplicaTransportInfo", ["LDAP"])
        , ("nsDS5ReplicaBindDN", ["uid=ldap/"++master++",ou=People,dc=scripts,dc=mit,dc=edu"])
        , ("nsDS5ReplicaBindMethod", ["SASL/GSSAPI"])
        , ("nsDS5ReplicaUpdateSchedule", ["0000-2359 0123456"])
        , ("nsDS5ReplicaTimeout", ["120"])
        ]

printBinds ldap = do
    binds <- getBinds ldap
    mapM_ putStrLn binds

suspendBinds ldap statefile = do
    binds <- getBinds ldap
    when (null binds) $
        error "suspendBinds: Cowardly refusing to write empty binds file"
    withFile statefile WriteMode $ \h ->
        hPutStr h (show binds)
    ldapModify ldap replicaBase [LDAPMod LdapModDelete "nsDS5ReplicaBindDN" []]

restoreBinds ldap statefile = do
    binds <- fmap read (readFile statefile)
    setBinds ldap binds

setBinds ldap binds = do
    oldBinds <- getBinds ldap
    when (null oldBinds) $
        error "setBinds: Cowardly refusing to overwrite non-empty binds on server"
    ldapModify ldap replicaBase [LDAPMod LdapModAdd "nsDS5ReplicaBindDN" binds]

forEachRawAgreement ldap f = do
    rawAgreements <- getRawAgreements ldap
    let width = maximum (0:concatMap (map length . lookupKey "nsDS5ReplicaHost" . leattrs) rawAgreements)
    forM_ rawAgreements (f width)

printStatus ldap = forEachRawAgreement ldap f >> printConflicts ldap
    where f width (LDAPEntry dn attrs) = do
            let mhost   = lookupKey1 "nsDS5ReplicaHost" attrs
                mstatus = lookupKey1 "nsds5replicaLastUpdateStatus" attrs
                minitstatus = lookupKey1 "nsds5replicaLastInitStatus" attrs
            case mhost of
                (Just host) -> do
                    let status = maybe "no status found" id mstatus
                    printf ("%-" ++ show width ++ "s : %s\n") host status
                _ -> warnIO ("Malformed replication agreement at " ++ dn)
            case minitstatus of
                (Just initstatus) -> putStrLn $ take width (repeat ' ') ++ " > " ++ initstatus
                _ -> return ()

printRUV ldap = forEachRawAgreement ldap f
    where f _ (LDAPEntry dn attrs) = do
            let mhost = lookupKey1 "nsDS5ReplicaHost" attrs
                ruvs = lookupKey "nsDS50ruv" attrs
            putStrLn (maybe dn id mhost)
            mapM_ (putStrLn . ("  " ++)) ruvs
            return ()
cleanRUV ldap = do
    error "Not implemented yet"

printConflicts ldap = do
    conflicts <- getConflicts ldap
    forM_ conflicts $ \(LDAPEntry dn _) ->
        putStrLn dn

getTarget ldap target = do
    dnMatch <- getEntry ldap target
    case dnMatch of
        (Just entry) -> return entry
        Nothing -> do
            r <- searchReplica ldap $ "(&(objectClass=nsDS5ReplicationAgreement)(nsDS5ReplicaHost=" ++ target ++ "))"
            case r of
                []  -> error "getTarget: target not found"
                [x] -> return x
                (length -> l) -> error $ printf "getTarget: target is ambiguous, found %d results" l

update ldap target = do
    (LDAPEntry dn attrs) <- getTarget ldap target
    -- check and make sure full updates are not broken
    let bindMethod = lookupKey1 "nsDS5ReplicaBindMethod" attrs
    version <- getVersion ldap
    {-
    case bindMethod of
        (Just "SASL/GSSAPI")
            | "389-Directory/1.2.6" == version ||
              isPrefixOf "389-Directory/1.2.6." version ->
                 error $ "update: GSSAPI full updates from 1.2.6 are broken,\n" ++
                 "        see https://bugzilla.redhat.com/show_bug.cgi?id=637852"
        _ -> return ()
    -}
    ldapModify ldap dn [LDAPMod LdapModAdd "nsDS5BeginReplicaRefresh" ["start"]]
    threadDelay 1000000
    printStatus ldap
    --updateMonitor

updateMonitor ldap target = do
    -- 5 second interval
    threadDelay 5000000
    (LDAPEntry _ attrs) <- getTarget ldap target
    -- XXX Find the status and report it, or exit
    updateMonitor ldap target
printVersion ldap = getVersion ldap >>= putStrLn

-- XXX not concurrent
testReplication ldap = do
    resetTestReplication ldap
    time <- formatTime defaultTimeLocale "%a-%b-%e-%H%M%S" `fmap` getCurrentTime
    ldapAdd ldap testDn $ list2ldm LdapModAdd
        [ ("objectClass",           ["top", "scriptsVhost"])
        , ("scriptsVhostName",      ["replication-test"])
        , ("scriptsVhostAlias",     ["replication-test-" ++ time])
        , ("scriptsVhostAccount",   ["uid=signup,ou=People,dc=scripts,dc=mit,dc=edu"])
        , ("scriptsVhostDirectory", [""])
        ]

setPluginEnabled ldap name status =
    ldapModify ldap ("cn=" ++ name ++ ",cn=plugins,cn=config")
        [LDAPMod LdapModReplace "nsslapd-pluginEnabled" [if status then "on" else "off"]]

disableReplication ldap = do
    setPluginEnabled ldap "Legacy Replication Plugin" False
    setPluginEnabled ldap "Multimaster Replication Plugin" False
enableReplication ldap = do
    setPluginEnabled ldap "Multimaster Replication Plugin" True
    setPluginEnabled ldap "Legacy Replication Plugin" True

setSyntaxCheck ldap status =
    ldapModify ldap ("cn=config")
        [LDAPMod LdapModReplace "nsslapd-syntaxcheck" [if status then "on" else "off"]]

disableSyntaxCheck ldap = setSyntaxCheck ldap False
enableSyntaxCheck ldap = setSyntaxCheck ldap True

resetTestReplication ldap = do
    old <- getEntry ldap testDn
    when (isJust old) $ ldapDelete ldap testDn
    conflicts <- getConflicts ldap
    forM_ conflicts $ \(LDAPEntry dn _) -> do
        let orig = tail (dropWhile (/= '+') dn)
        when (normalizeKey orig == normalizeKey testDn) $ do
            -- Work around deadlock in multimaster replication
            ldapModify ldap dn [LDAPMod LdapModDelete "nsds5replconflict" []]
            ldapDelete ldap dn

recoverUser ldap uid = do
    ldapAdd ldap "dc=scripts,dc=mit,dc=edu" (list2ldm LdapModAdd
        [ ("objectClass", ["top", "domain"])
        , ("dc", ["scripts"])
        ]) `catch` (\(e :: SomeException) -> print e)
    ldapAdd ldap "ou=People,dc=scripts,dc=mit,dc=edu" (list2ldm LdapModAdd
        [ ("objectClass", ["top", "organizationalunit"])
        , ("ou", ["People"])
        ]) `catch` (\(e :: SomeException) -> print e)
    ldapAdd ldap ("uid=" ++ uid ++ ",ou=People,dc=scripts,dc=mit,dc=edu") (list2ldm LdapModAdd
        [ ("objectClass", ["top", "account"])
        , ("uid", [uid])
        ]) `catch` (\(e :: SomeException) -> print e)

-------------------------------------------------------------------------------
-- Option parsing

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
putOptPassword p r = r { optPassword = maybe AskPassword Password p }

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

    where header = "Usage: mmr-hammer [status|test|...] ...\n" ++
                   "\n" ++
                   "mmr-hammer is a command line tool for managing multimaster replication\n" ++
                   "on Fedora 389 DS.\n" ++
                   "\n" ++
                   "Advanced commands: suspend, restore, set, reinit, disable, enable,\n" ++
                   "                   agreements, binds, ruv, version, update, reset, recover"

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

-------------------------------------------------------------------------------
-- Debugging

isDebugging = unsafePerformIO (newIORef False)
{-# NOINLINE isDebugging #-}

debugIO msg = do
    b <- readIORef isDebugging
    when b (hPutStrLn stderr $ "DEBUG: " ++ msg)
debugIOVal msg m = do
    debugIO msg
    m
warnIO msg = hPutStrLn stderr $ "WARNING: " ++ msg

usage v = putStrLn ("Usage: mmr-hammer " ++ v)

-------------------------------------------------------------------------------
-- Command dispatch

main = do
    (opts, args) <- parseOptions
    when (optDebug opts) (writeIORef isDebugging True)
    (uri, user, password) <- tryAll $ [defaultStrategy, ldapVircStrategy, fallbackStrategy] `ap` [opts]
    uristruct <- case parseURI uri of
        Nothing -> error "Malformed URI"
        Just uristruct -> return uristruct
    let host = maybe "none" uriRegName (uriAuthority uristruct)
        replicasFile = "mmr-hammer-replica-" ++ host
        bindsFile    = "mmr-hammer-binds-" ++ host
    ldap <- createLdap uri user password
    case args of
        ["binds"] -> printBinds ldap
        ["agreements"] -> printAgreements ldap
        ["suspend", "agreements"] -> suspendAgreements ldap replicasFile
        ["suspend", "binds"] -> suspendBinds ldap bindsFile
        ["restore", "agreements"] -> restoreAgreements ldap replicasFile
        ["restore", "binds"] -> restoreBinds ldap bindsFile
        ["set", "binds"] -> usage "set binds uid=ldap/example.com,ou=People,dn=example,dn=com ..."
        ("set": "binds": binds) -> setBinds ldap binds
        ["reinit", "agreements"] -> reinitAgreements ldap replicasFile
        ["disable", "replication"] -> disableReplication ldap
        ["disable", "syntaxcheck"] -> disableSyntaxCheck ldap
        ["enable", "replication"] -> enableReplication ldap
        ["enable", "syntaxcheck"] -> enableSyntaxCheck ldap
        ["status"]  -> printStatus ldap
        ["version"] -> printVersion ldap
        ["update", target] -> update ldap target
        ["test"] -> testReplication ldap
        ["reset", "test"] -> resetTestReplication ldap
        ["ruv"] -> printRUV ldap
        ["recover", "user", uid] -> recoverUser ldap uid
        ["cleanruv", target, replicaid] -> cleanRUV ldap target replicaid
        ["conflicts"] -> printConflicts ldap
        ["rrr"] -> removeRedundantReplication ldap
        ("init": "agreements": targets) -> initAgreements ldap targets
        ("suspend": _) -> usage "suspend [agreements|binds]"
        ("set":     _) -> usage "set [binds] VALUES..."
        ("restore": _) -> usage "restore [agreements|binds]"
        ("init":    _) -> usage "init [agreements]"
        ("reinit":  _) -> usage "reinit [agreements]"
        ("disable": _) -> usage "disable [replication|syntaxcheck]"
        ("enable":  _) -> usage "enable [replication|syntaxcheck]"
        ("reset":   _) -> usage "reset [test]"
        ("recover": _) -> usage "recover [user]"
        ("cleanruv":_) -> usage "cleanruv TARGET REPLICAID"
        _ -> error "Unknown command"
