import { useEffect, useState, useSyncExternalStore, type ReactNode } from "react";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { appSettingsSchema } from "../../src/rpc/schemas";
import { rpc, useTRPC } from "./rpc";
import { LibraryMigrationPreview } from "./library-migration";
import { applySettingsDraft, changeSetting, conflictingSettings, rebaseSettingsDraft, type SettingsDraft } from "./settings-draft";
import { BookSettings, DownloadSettings, IndexerSettings, MovieSettings, NotificationSettings, PlexSettings,
  SettingsGroup, StorageSettings, TvSettings, type SettingsForm } from "./settings-fields";
import "./settings.css";

const draftKey = ["admin-settings-draft"] as const;
const saveKey = ["admin-settings-save"] as const;
const selectKey = ["admin-settings-plex-select"] as const;
const emptyDraft: SettingsDraft = {};
const sections = {
  books: { title: "Books & feed", description: "Library paths, reader feed and edition matching.", roots: ["libraryRoot", "feed", "agents"] },
  indexers: { title: "Indexers", description: "Manage search providers and release sources.", roots: ["prowlarr", "torznab"] },
  downloads: { title: "Downloads", description: "Torrent connection, polling and recovery.", roots: ["rtorrent", "polling", "recovery"] },
  movies: { title: "Movies", description: "Publishing, request integrations and release quality.", roots: ["movies"] },
  tv: { title: "TV", description: "Series publishing, Sonarr and path mappings.", roots: ["tv"] },
  plex: { title: "Plex access", description: "Choose who can sign in and configure reader app redirects.", roots: ["auth"] },
  storage: { title: "Storage", description: "Set retention limits and preview an existing library.", roots: ["retention"] },
  notifications: { title: "Notifications", description: "Configure alerts delivered through Pushover.", roots: ["notifications"] },
} satisfies Record<string, { title: string; description: string; roots: string[] }>;
type Section = keyof typeof sections;
const sectionKeys = Object.keys(sections) as Section[];
const sectionFor = (path: string) => sectionKeys.find(key => sections[key].roots.includes(path.split(".")[0]!));

function PlexAccess({ form, saving }: { form: SettingsForm; saving: boolean }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const plex = useQuery(trpc.adminWeb.plexServers.queryOptions({}, { refetchOnWindowFocus: false }));
  const [pendingMachine, setPendingMachine] = useState<string | null>(null);
  const selecting = useIsMutating({ mutationKey: selectKey }) > 0;
  const select = useMutation({ ...trpc.adminWeb.selectPlexServer.mutationOptions(), mutationKey: selectKey,
    onSuccess: async ({ selectedMachineId }) => {
      await cache.cancelQueries({ queryKey: trpc.settings.get.queryKey() });
      cache.setQueryData(trpc.settings.get.queryKey({}), current => current && ({
        ...current, auth: { ...current.auth, plex: { ...current.auth.plex, machineId: selectedMachineId } },
      }));
      setPendingMachine(null);
      await Promise.all([
        cache.invalidateQueries({ queryKey: trpc.settings.get.queryKey() }),
        cache.invalidateQueries({ queryKey: trpc.adminWeb.plexServers.queryKey() }),
      ]);
    },
  });
  const selected = plex.data?.selectedMachineId ?? "";
  const candidate = pendingMachine ?? selected;
  const manualDraft = !!form.draft["auth.plex.machineId"];
  const listed = plex.data?.servers.some(server => server.machineId === candidate);
  const active = plex.data?.servers.find(server => server.machineId === selected);
  const tokenStatus = !plex.data?.ownerTokenConfigured ? "Not configured" : plex.data.ownerTokenExpired === true
    ? "Expired; sign in again to relink" : plex.data.ownerTokenExpired === false ? "Configured, not expired" : "Configured; expiry unknown";
  return <>
    <SettingsGroup title="Plex access control" note="Select which Plex server controls browser sign-ins. Applying this selection does not save any other draft settings.">
      {plex.isPending ? <p role="status">Loading accessible Plex servers...</p> : plex.isError ? <p role="alert">{plex.error.message}</p> : <>
        <dl className="settings-facts">
          <div><dt>Current server</dt><dd>{active?.name ?? (selected || "Not selected")}</dd></div>
          <div><dt>Owner token</dt><dd>{tokenStatus}</dd></div>
        </dl>
        {plex.data.error && <p role="alert">{plex.data.error}</p>}
        {plex.data.source === "stale-cache" && <p className="settings-note">Showing the last known server list. Podible verifies the server again before applying a selection.</p>}
        <div className="settings-plex-picker">
          <div className="settings-field">
            <label htmlFor="admin-plex-server">Plex access server</label>
            <select id="admin-plex-server" value={candidate} disabled={selecting || saving || !plex.data.servers.length}
              onChange={event => { setPendingMachine(event.target.value); select.reset(); }}>
              {!listed && <option value={candidate}>{candidate || "Select a server"}</option>}
              {plex.data.servers.map(server => <option key={server.machineId} value={server.machineId}>
                {server.name}{server.owned ? " (owned)" : server.sourceTitle ? ` (shared by ${server.sourceTitle})` : " (shared)"}
              </option>)}
            </select>
          </div>
          <button type="button" disabled={!listed || candidate === selected || manualDraft || selecting || saving}
            onClick={() => {
              if (!cache.isMutating({ mutationKey: selectKey }) && !cache.isMutating({ mutationKey: saveKey })) select.mutate({ machineId: candidate });
            }}>{selecting ? "Selecting..." : "Use selected Plex server"}</button>
        </div>
        {!plex.data.servers.length && <p className="settings-note">No accessible servers reported. The existing selection is preserved.</p>}
        {manualDraft && <p className="settings-note">Save or discard the manual machine ID edit before using the verified picker.</p>}
      </>}
      <button type="button" className="settings-text-button" disabled={plex.isFetching || selecting} onClick={() => void plex.refetch()}>Reload Plex server list</button>
      {select.error && <p role="alert">Plex selection was not changed: {select.error.message}</p>}
      {select.isSuccess && <p role="status">Plex access server selected. Other unsaved settings are preserved.</p>}
    </SettingsGroup>
    <fieldset className="settings-plex-fields" disabled={selecting}><PlexSettings form={form} /></fieldset>
  </>;
}

export default function Settings({ navigation }: { navigation: ReactNode }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const settingsKey = trpc.settings.get.queryKey({});
  const saving = useIsMutating({ mutationKey: saveKey }) > 0;
  const selecting = useIsMutating({ mutationKey: selectKey }) > 0;
  const settings = useQuery(trpc.settings.get.queryOptions({}, { refetchOnWindowFocus: !saving && !selecting }));
  // The actor's QueryClient keeps the draft across SPA navigation, never in browser storage.
  useQuery<SettingsDraft>({
    queryKey: draftKey, queryFn: async () => emptyDraft, enabled: false, initialData: emptyDraft, gcTime: Infinity, notifyOnChangeProps: [],
  });
  const [draftStore] = useState(() => ({
    subscribe: (change: () => void) => cache.getQueryCache().subscribe(event => {
      if (event.query.queryKey[0] === draftKey[0]) change();
    }),
    snapshot: () => cache.getQueryData<SettingsDraft>(draftKey) ?? emptyDraft,
  }));
  // Query observer notifications are batched; controlled inputs need the draft immediately.
  const draft = useSyncExternalStore(draftStore.subscribe, draftStore.snapshot);
  const [section, setSection] = useState<Section>("books");
  const save = useMutation({
    mutationKey: saveKey,
    async mutationFn(submitted: SettingsDraft) {
      // The API replaces the whole object; start with a fresh read to carry forward untouched fields.
      const latest = await cache.fetchQuery(trpc.settings.get.queryOptions({}, { staleTime: 0 }));
      if (conflictingSettings(latest, submitted).length) return null;
      const changes = rebaseSettingsDraft(latest, submitted);
      if (!Object.keys(changes).length) return { settings: latest, written: false };
      const checked = appSettingsSchema.safeParse(applySettingsDraft(latest, changes));
      if (!checked.success) return null;
      return { settings: await rpc.settings.update.mutate({ settings: checked.data }), written: true };
    },
    async onSuccess(result, submitted) {
      if (!result) return;
      await cache.cancelQueries({ queryKey: settingsKey });
      cache.setQueryData(settingsKey, result.settings);
      cache.setQueryData<SettingsDraft>(draftKey, current => rebaseSettingsDraft(result.settings, current ?? emptyDraft, submitted));
      if (result.written) void cache.invalidateQueries({ queryKey: trpc.adminWeb.plexServers.queryKey() });
    },
  });
  const saved = settings.data;
  const conflicts = saved ? conflictingSettings(saved, draft) : [];
  const dirty = Object.keys(draft).length > 0;
  const value = saved && applySettingsDraft(saved, draft);
  const parsed = value && appSettingsSchema.safeParse(value);
  const issues = parsed && !parsed.success ? parsed.error.issues.map(issue => ({
    path: issue.path.join("."),
    message: issue.code === "invalid_type" && issue.expected === "number" ? "Enter a valid number." : issue.message,
  })) : [];
  const firstIssueSection = issues[0] && sectionFor(issues[0].path);

  useEffect(() => {
    if (!dirty) return;
    const beforeUnload = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ""; };
    window.addEventListener("beforeunload", beforeUnload);
    return () => window.removeEventListener("beforeunload", beforeUnload);
  }, [dirty]);

  function openSection(next: Section) {
    setSection(next);
    const panel = document.getElementById("settings-panel");
    if (next !== section && panel && panel.getBoundingClientRect().top < 0) panel.scrollIntoView({ block: "start" });
  }

  const form: SettingsForm | undefined = value && saved ? {
    value, saved, draft, issues,
    change(path, label, next, input) {
      const latest = cache.getQueryData<typeof saved>(settingsKey) ?? saved;
      const submitting = cache.isMutating({ mutationKey: saveKey }) > 0;
      cache.setQueryData<SettingsDraft>(draftKey, current => changeSetting(current ?? emptyDraft, latest, path, label, next, input, submitting));
      if (!submitting) save.reset();
    },
  } : undefined;

  const canSave = dirty && parsed?.success && !conflicts.length && !saving && !selecting && !settings.isFetching;

  return <div className="settings-page">
    <div className="settings-intro">
      <div>
        <p className="eyebrow">LIBRARY ADMINISTRATION</p>
        <h1>Settings</h1>
        <p>Manage how Podible connects, finds releases and publishes your library.</p>
      </div>
      {navigation}
    </div>
    <div className="settings-layout">
      <nav className="settings-index" aria-label="Settings sections">
        <p className="settings-kicker" aria-hidden="true">Configuration</p>
        {sectionKeys.map((key, index) => {
          const edited = Object.keys(draft).some(path => sectionFor(path) === key);
          return <button key={key} type="button" aria-current={section === key ? "true" : undefined} aria-controls="settings-panel"
            aria-description={edited ? "Unsaved changes" : undefined} onClick={() => openSection(key)}>
            <span className="settings-index-number" aria-hidden="true">0{index + 1}</span>
            <span>{sections[key].title}</span>
            {edited && <span className="settings-index-dot" aria-hidden="true" />}
          </button>;
        })}
      </nav>
      <div className="settings-main">
        <section className="settings-panel" id="settings-panel" aria-labelledby="settings-section-title">
          <div className="settings-panel-heading">
            <p className="settings-kicker">Section {sectionKeys.indexOf(section) + 1} of {sectionKeys.length}</p>
            <h2 id="settings-section-title">{sections[section].title}</h2>
            <p>{sections[section].description}</p>
          </div>
          {settings.isPending && <p className="settings-load" role="status">Loading settings...</p>}
          {settings.error && <p className="settings-load" role="alert">{settings.error.message}{dirty ? ". Your unsaved draft has been kept." : ""}</p>}
          {form && <div className="settings-panel-body">
            {section === "books" && <BookSettings form={form} />}
            {section === "indexers" && <IndexerSettings form={form} />}
            {section === "downloads" && <DownloadSettings form={form} />}
            {section === "movies" && <MovieSettings form={form} />}
            {section === "tv" && <TvSettings form={form} openMovies={() => openSection("movies")} />}
            {section === "plex" && <PlexAccess form={form} saving={saving} />}
            {section === "storage" && <><StorageSettings form={form} /><LibraryMigrationPreview /></>}
            {section === "notifications" && <NotificationSettings form={form} />}
          </div>}
        </section>
        <div className="settings-save" data-sticky={dirty} aria-label="Settings actions">
          <div className="settings-save-status">
            <p role="status" className="settings-save-state" data-dirty={dirty}>
              {saving ? "Saving changes..." : save.data?.written ? dirty ? "Settings saved; newer edits remain unsaved." : "Settings saved."
                : dirty ? "Unsaved changes" : saved ? "No unsaved changes" : "Settings not loaded"}
            </p>
            {conflicts.length > 0 && <div className="settings-save-message" role="alert">
              <p>Server settings changed while you were editing: {conflicts.map(([, edit]) => edit.label).join(", ")}. Choose which values to keep.</p>
              <div className="settings-conflict-actions">
                <button type="button" className="settings-text-button" disabled={saving || selecting || settings.isFetching}
                  onClick={() => saved && cache.setQueryData<SettingsDraft>(draftKey, current => rebaseSettingsDraft(saved, current ?? emptyDraft))}>Keep draft edits</button>
                <button type="button" className="settings-text-button" disabled={saving || selecting || settings.isFetching}
                  onClick={() => saved && cache.setQueryData<SettingsDraft>(draftKey, current => {
                    const next = { ...current };
                    for (const [path] of conflictingSettings(saved, next)) delete next[path];
                    return next;
                  })}>Use server values</button>
              </div>
            </div>}
            {issues.length > 0 && <p className="settings-save-message" role="alert">
              Check {issues.length === 1 ? "1 invalid setting" : `${issues.length} invalid settings`}{firstIssueSection ? <> in <button type="button" className="settings-text-button"
                onClick={() => openSection(firstIssueSection)}>{sections[firstIssueSection].title}</button></> : ""}.
            </p>}
            {save.error && <p className="settings-save-message" role="alert">Settings were not saved: {save.error.message}. Your draft has been kept.</p>}
          </div>
          <div className="settings-save-actions">
            <button type="button" className="settings-text-button" disabled={settings.isFetching || saving || selecting} onClick={() => void settings.refetch()}>
              {settings.isFetching ? "Reloading..." : "Reload server settings"}
            </button>
            {dirty && <button type="button" className="secondary" disabled={saving || selecting}
              onClick={() => { cache.setQueryData(draftKey, emptyDraft); save.reset(); }}>Discard draft</button>}
            <button type="button" disabled={!canSave} onClick={() => {
              if (canSave && !cache.isMutating({ mutationKey: saveKey }) && !cache.isMutating({ mutationKey: selectKey })) save.mutate(draft);
            }}>{saving ? "Saving..." : "Save settings"}</button>
          </div>
        </div>
      </div>
    </div>
  </div>;
}
