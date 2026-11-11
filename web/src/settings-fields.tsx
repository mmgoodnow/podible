import { useState, type ReactNode } from "react";
import type { AppSettings, TorznabSource } from "../../src/app-types";
import { numericSetting, settingAt, type SettingsDraft } from "./settings-draft";

type Issue = { path: string; message: string };
export type SettingsForm = {
  value: AppSettings;
  saved: AppSettings;
  draft: SettingsDraft;
  issues: Issue[];
  change(path: string, label: string, value: unknown, input?: string): void;
};

type Field<T> = {
  path: string;
  value: T;
  persisted: T;
  input?: string;
  issue?: string;
  set(value: T, label: string, input?: string): void;
};

export function fields<T = AppSettings>(form: SettingsForm, value: T = form.value as T, parent = "") {
  const pathFor = (key: string) => parent ? `${parent}.${key}` : key;
  return {
    group<K extends keyof T & string>(key: K) { return fields(form, value[key], pathFor(key)); },
    field<K extends keyof T & string>(key: K): Field<T[K]> {
      const path = pathFor(key);
      const current = value[key];
      const list = Array.isArray(current) && (typeof current[0] === "string" || typeof current[0] === "number");
      return { path, value: current, persisted: settingAt(form.saved, path) as T[K], input: form.draft[path]?.input,
        issue: form.issues.find(issue => issue.path === path || (list && issue.path.startsWith(`${path}.`)))?.message,
        set: (next, label, input) => form.change(path, label, next, input) };
    },
  };
}

function fieldId(field: Pick<Field<unknown>, "path">) {
  return `settings-${field.path.replaceAll(".", "-")}`;
}

function Frame({ field, label, help, kind, children }: {
  field: Pick<Field<unknown>, "path" | "issue">; label: string; help: string; kind?: "check";
  children(props: { id: string; "aria-describedby"?: string; "aria-invalid"?: true }): ReactNode;
}) {
  const id = fieldId(field);
  return <div className="settings-field" data-kind={kind}>
    <div className="settings-field-copy">
      <label htmlFor={id}>{label}</label>
      <small id={`${id}-help`}>{help}</small>
    </div>
    <div className="settings-field-control">
      {children({ id, "aria-describedby": field.issue ? `${id}-help ${id}-error` : `${id}-help`,
        "aria-invalid": field.issue ? true : undefined })}
      {field.issue && <small id={`${id}-error`} className="settings-field-error">{field.issue}</small>}
    </div>
  </div>;
}

export function TextField({ field, label, help, placeholder, nullable }: {
  field: Field<string | null | undefined>; label: string; help: string; placeholder?: string; nullable?: boolean;
}) {
  return <Frame {...{ field, label, help }}>{props => <input {...props} type="text" spellCheck={false} autoComplete="off"
    placeholder={placeholder} value={field.value ?? ""} onChange={event => field.set(event.target.value || (nullable ? null : ""), label)} />}</Frame>;
}

function SecretField({ field, label, help }: { field: Field<string | undefined>; label: string; help: string }) {
  const [replacement, setReplacement] = useState<{ before: string | undefined; text: string }>();
  const active = replacement && replacement.text === field.value && field.value !== field.persisted ? replacement : undefined;
  return <Frame {...{ field, label, help }}>{props => <>
    <div className="settings-secret">
      <input {...props} type="password" autoComplete="new-password" spellCheck={false} value={active?.text ?? ""}
        placeholder={field.value ? "\u2022".repeat(8) : "Enter a value"}
        onChange={event => {
          const text = event.target.value, before = active ? active.before : field.value;
          setReplacement(text ? { before, text } : undefined);
          field.set(text || before, label);
        }} />
      <button type="button" className="secondary" aria-label={`Clear ${label}`} disabled={!field.value}
        onClick={() => { setReplacement(undefined); field.set("", label); }}>Clear</button>
    </div>
    <small>{active ? "Replacement entered" : field.value ? "Value set" : "No value set"}</small>
  </>}</Frame>;
}

function hasUrlCredentials(value: string) {
  try {
    const url = new URL(value);
    return !!(url.username || url.password) || [...url.searchParams.keys()].some(key =>
      /^(?:api[-_]?key|key|(?:access[-_]?)?token|passkey|password|auth|secret|x-plex-token)$/i.test(key));
  } catch { return false; }
}

function EndpointField({ field, label, help, placeholder, storedUrls }: {
  field: Field<string>; label: string; help: string; placeholder?: string; storedUrls?: ReadonlySet<string>;
}) {
  return (hasUrlCredentials(field.persisted) || storedUrls?.has(field.value))
    ? <SecretField field={field} label={label} help={`${help} The saved URL is masked because it contains credentials.`} />
    : <TextField {...{ field, label, help, placeholder }} />;
}

const GiB = 1024 ** 3;

function NumberField({ field, label, help, nullable, scale = 1, min }: {
  field: Field<number | null>; label: string; help: string; nullable?: boolean; scale?: number; min?: number;
}) {
  const [typing, setTyping] = useState<{ value: number | null; input: string }>();
  const input = field.input ?? (typing && Object.is(typing.value, field.value) ? typing.input : undefined);
  return <Frame {...{ field, label, help }}>{props => <input {...props} type="text" inputMode={scale === 1 ? "numeric" : "decimal"}
    autoComplete="off" placeholder={nullable ? "Not set" : undefined} value={input ?? (field.value === null ? "" : field.value / scale)}
    onChange={event => {
      const input = event.target.value, parsed = numericSetting(input, nullable);
      const value = parsed === null ? null : scale === 1 ? parsed : Math.round(parsed * scale);
      const valid = value === null || ((min === undefined || value >= min) && Number.isSafeInteger(value));
      const next = valid ? value : NaN;
      setTyping({ value: next, input });
      field.set(next, label, input);
    }} />}</Frame>;
}

function CheckField({ field, label, help }: { field: Field<boolean>; label: string; help: string }) {
  return <Frame {...{ field, label, help }} kind="check">{props => <input {...props} type="checkbox" checked={field.value}
    onChange={event => field.set(event.target.checked, label)} />}</Frame>;
}

function SelectField<T extends string | number>({ field, label, help, options }: {
  field: Field<T>; label: string; help: string; options: readonly (readonly [T, string])[];
}) {
  return <Frame {...{ field, label, help }}>{props => <select {...props} value={field.value}
    onChange={event => {
      const option = options.find(([value]) => String(value) === event.target.value);
      if (option) field.set(option[0], label);
    }}>
    {options.map(([value, name]) => <option key={value} value={value}>{name}</option>)}
  </select>}</Frame>;
}

function ListField({ field, label, help }: { field: Field<string[]>; label: string; help: string }) {
  const [typing, setTyping] = useState<{ value: string[]; input: string }>();
  const input = field.input ?? (typing && typing.value.length === field.value.length &&
    field.value.every((value, index) => Object.is(value, typing.value[index])) ? typing.input : undefined);
  return <Frame {...{ field, label, help }}>{props => <textarea {...props} rows={3} spellCheck={false} autoComplete="off"
    value={input ?? field.value.join("\n")}
    onChange={event => {
      const input = event.target.value, lines = input.split("\n").map(line => line.trim()).filter(Boolean);
      setTyping({ value: lines, input });
      field.set(lines, label, input);
    }} />}</Frame>;
}

function Grid({ children }: { children: ReactNode }) {
  return <div className="settings-grid">{children}</div>;
}

export function SettingsGroup({ title, note, children }: { title: string; note?: string; children: ReactNode }) {
  return <section className="settings-group" aria-label={title}>
    <h3>{title}</h3>
    {note && <p className="settings-note">{note}</p>}
    {children}
  </section>;
}

function Rows<T>({ field, label, singular, empty, create, ordered, children }: {
  field: Field<T[]>; label: string; singular: string; empty: string; create(): T; ordered?: boolean;
  children(row: T, change: (row: T) => void, index: number): ReactNode;
}) {
  const move = (index: number, target: number) => {
    const rows = [...field.value];
    [rows[index], rows[target]] = [rows[target]!, rows[index]!];
    field.set(rows, label);
  };
  return <div className="settings-rows">
    {!field.value.length && <p className="settings-empty">{empty}</p>}
    {field.value.map((row, index) => <fieldset className="settings-row" key={index}>
      <legend>{singular} {index + 1}</legend>
      <Grid>{children(row, next => field.set(field.value.map((entry, at) => at === index ? next : entry), label), index)}</Grid>
      <div className="settings-row-actions">
        {ordered && <>
          <button type="button" className="secondary" aria-label={`Move ${singular.toLowerCase()} ${index + 1} up`}
            disabled={index === 0} onClick={() => move(index, index - 1)}>Move up</button>
          <button type="button" className="secondary" aria-label={`Move ${singular.toLowerCase()} ${index + 1} down`}
            disabled={index === field.value.length - 1} onClick={() => move(index, index + 1)}>Move down</button>
        </>}
        <button type="button" className="secondary" aria-label={`Remove ${singular.toLowerCase()} ${index + 1}`}
          onClick={() => field.set(field.value.filter((_, at) => at !== index), label)}>Remove</button>
      </div>
    </fieldset>)}
    {field.issue && <p className="settings-field-error">{field.issue}</p>}
    <button type="button" className="secondary" onClick={() => field.set([...field.value, create()], label)}>Add {singular.toLowerCase()}</button>
  </div>;
}

function rowField<T>(form: SettingsForm, collection: string, index: number, key: string, value: T, set: (value: T) => void): Field<T> {
  const path = `${collection}.${index}.${key}`;
  return { path, value, persisted: settingAt(form.saved, path) as T, set, issue: form.issues.find(issue => issue.path === path)?.message };
}

export function BookSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), feed = root.group("feed"), agents = root.group("agents");
  return <>
    <SettingsGroup title="Reader feed">
      <Grid>
        <TextField label="Feed title" field={feed.field("title")} help="Name shown for your library in feed readers." />
        <TextField label="Feed author" field={feed.field("author")} help="Author or publisher credited on the feed." />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Edition matching">
      <EditionPreference field={agents.field("editionPreference")} />
    </SettingsGroup>
  </>;
}

function EditionPreference({ field }: { field: Field<string> }) {
  const label = "Edition preference";
  return <Frame field={field} label={label} help="Tell Podible which editions you prefer, such as full-cast audio or EPUB.">
    {props => <textarea {...props} rows={4} value={field.value} onChange={event => field.set(event.target.value, label)} />}
  </Frame>;
}

export function AiSettings({ form }: { form: SettingsForm }) {
  const agents = fields(form).group("agents");
  return <SettingsGroup title="OpenAI">
    <Grid>
      <SecretField label="OpenAI API key" field={agents.field("apiKey")} help="Authorizes OpenAI calls for release selection and other AI tasks." />
      <NumberField label="Agent timeout (ms)" field={agents.field("timeoutMs")} min={1000}
        help="Default for agent calls. Longer-running tasks can use their own limits." />
    </Grid>
  </SettingsGroup>;
}

type DiscoveredIndexers = {
  state: "not-configured" | "available" | "unavailable";
  indexers: { id: number; name: string; search: string[]; feeds: string[]; retryAt: number | null }[];
  duplicates: { index: number; name: string }[];
};
const indexerMedia: Record<string, string> = { audio: "Audiobooks", ebook: "Ebooks", movie: "Movies", tv: "TV" };
const formatIndexerMedia = (media: string[]) => media.map(kind => indexerMedia[kind]).join(", ") || "None";

export function IndexerSettings({ form, discovery, loading }: { form: SettingsForm; discovery?: DiscoveredIndexers; loading: boolean }) {
  const root = fields(form), prowlarr = root.group("prowlarr"), sources = root.field("torznab");
  const original = form.draft.torznab?.original as TorznabSource[] | undefined;
  const protectedUrls = new Set([...(original ?? []), ...form.saved.torznab].map(source => source.baseUrl).filter(hasUrlCredentials));
  return <>
    <SettingsGroup title="Prowlarr">
      <Grid><EndpointField label="Prowlarr URL" field={prowlarr.field("url")} placeholder="https://prowlarr.example"
        help="Address Podible uses to discover and query your indexers. Include any URL base." />
        <SecretField label="Prowlarr API key" field={prowlarr.field("apiKey")} help="Authorizes discovery, searches, and feed requests." /></Grid>
    </SettingsGroup>
    {form.saved.prowlarr.url && <SettingsGroup title="Discovered indexers">
      {loading ? <p role="status">Loading indexers...</p> : discovery?.state !== "available"
        ? <p>Prowlarr discovery is unavailable. Check the saved URL and API key.</p>
        : discovery.indexers.length === 0 ? <p>No enabled torrent indexers were discovered.</p>
        : <table className="settings-indexer-table" aria-label="Discovered indexers">
          <thead><tr><th scope="col">Indexer</th><th scope="col">Search</th><th scope="col">Feeds</th></tr></thead>
          <tbody>{discovery.indexers.map(indexer => <tr key={indexer.id}>
            <th scope="row">{indexer.name}{indexer.retryAt !== null && <small className="settings-indexer-cooldown">Cooling down</small>}</th>
            <td>{formatIndexerMedia(indexer.search)}</td>
            <td>{formatIndexerMedia(indexer.feeds)}</td>
          </tr>)}</tbody>
        </table>}
    </SettingsGroup>}
    <SettingsGroup title="Torznab sources" note={form.saved.prowlarr.url
      ? "Prowlarr is active. Manual sources are saved but not queried."
      : "Configure direct sources to use without Prowlarr."}>
      <Rows<TorznabSource> field={sources} label="Torznab sources" singular="Torznab source" empty="No direct Torznab sources configured."
        create={() => ({ name: "", baseUrl: "" })}>
        {(source, change, index) => <>
          {source.baseUrl === form.saved.torznab[index]?.baseUrl && discovery?.duplicates.find(row => row.index === index) &&
            <p className="settings-note settings-duplicate-source">Already managed by Prowlarr. You can remove this manual entry.</p>}
          <TextField label="Source name" help="Name shown for this indexer in Podible."
            field={rowField(form, sources.path, index, "name", source.name, name => change({ ...source, name }))} />
          <EndpointField label="Torznab URL" storedUrls={protectedUrls}
            help="Torznab endpoint supplied by the indexer."
            field={rowField(form, sources.path, index, "baseUrl", source.baseUrl, baseUrl => change({ ...source, baseUrl }))} />
          <SecretField label="Torznab API key" help="Key for this endpoint, if the indexer requires one."
            field={rowField(form, sources.path, index, "apiKey", source.apiKey, apiKey => change({ ...source, apiKey }))} />
          <TextField label="Audiobook categories" field={rowField(form, sources.path, index, "categories.audio", source.categories?.audio,
            audio => change({ ...source, categories: { ...source.categories, audio } }))} help="Category IDs, separated by commas." />
          <TextField label="Ebook categories" field={rowField(form, sources.path, index, "categories.ebook", source.categories?.ebook,
            ebook => change({ ...source, categories: { ...source.categories, ebook } }))} help="Category IDs, separated by commas." />
        </>}
      </Rows>
    </SettingsGroup>
  </>;
}

export function AcquisitionSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), movies = root.group("movies"), tv = root.group("tv");
  return <>
    <SettingsGroup title="Release selection">
      <Grid>
        <NumberField label="Maximum movie release size (GiB)" field={movies.field("maximumReleaseBytes")} scale={GiB} min={1} nullable
          help="Skip movie releases larger than this. Leave blank for no cap." />
        <NumberField label="Maximum TV release size (GiB)" field={tv.field("maximumReleaseBytes")} scale={GiB} min={1} nullable
          help="Skip TV releases larger than this, including packs. Leave blank for no cap." />
        <SelectField label="Movie cross-seed preference" field={movies.field("crossSeedPreference")} options={[["off", "Off"], ["tie-break", "Prefer reuse as a tie-breaker"]]}
          help="Use cross-seed potential to break ties between otherwise comparable movie releases." />
        <CheckField label="Automatically upgrade movies" field={movies.field("automaticUpgrades")}
          help="Let better releases replace existing movie copies when they meet your quality rules." />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Download files">
      <NumberField label="Movie download group ID" field={movies.field("downloadGroupId")} nullable
        help="Filesystem group for new movie download folders. Leave blank to keep the default." />
    </SettingsGroup>
    <SettingsGroup title="Download recovery">
      <Grid>
        <NumberField label="Stalled torrent threshold (minutes)" field={root.group("recovery").field("stalledTorrentMinutes")} min={0}
          help="How long a download can stall before recovery. Set to 0 to recover as soon as a stall is detected." />
      </Grid>
    </SettingsGroup>
  </>;
}

const tierSources = [["web", "WEB"], ["bluray", "Blu-ray"], ["hdtv", "HDTV"], ["dvd", "DVD"], ["cam", "CAM"]] as const;
const tierResolutions = [[480, "480p"], [720, "720p"], [1080, "1080p"], [2160, "2160p"]] as const;

export function SeerrSettings({ form }: { form: SettingsForm }) {
  const seerr = fields(form).group("movies").group("seerr");
  return <SettingsGroup title="Connection">
    <Grid>
      <EndpointField label="Seerr URL" field={seerr.field("url")} help="Address Podible uses to read requests from Seerr." />
      <SecretField label="Seerr API key" field={seerr.field("apiKey")} help="Authorizes reading requests and metadata from Seerr." />
      <NumberField label="Seerr movie sync interval (minutes)" field={seerr.field("pollIntervalMinutes")} help="How often Podible checks Seerr for new movie requests." />
    </Grid>
  </SettingsGroup>;
}

export function QualitySettings({ form }: { form: SettingsForm }) {
  const quality = fields(form).group("movies").group("qualityPolicy"), tiers = quality.field("tiers");
  return <>
    <SettingsGroup title="Video preferences">
      <Grid>
        <TextField label="Required cut" field={quality.field("cut")} nullable help="Require a cut such as theatrical. Leave blank to allow any cut." />
        <NumberField label="Minimum preference gain" field={quality.field("minimumPreferenceGain")} help="Minimum score improvement for a same-tier upgrade." />
        <ListField label="Audio languages" field={quality.field("audioLanguages")} help="Allowed audio languages, one code per line (such as en). Leave blank to allow any." />
        <ListField label="Allowed codecs" field={quality.field("allowedCodecs")} help="Eligible video codecs, one per line (such as h264, hevc, or av1)." />
        <ListField label="Preferred codecs" field={quality.field("preferredCodecs")} help="Rank eligible codecs, one per line, most preferred first." />
        <CheckField label="Allow HDR" field={quality.field("allowHdr")} help="Include high dynamic range releases when choosing a copy." />
        <CheckField label="Prefer proper and repack releases" field={quality.field("preferProper")} help="Favor corrected releases over otherwise comparable copies." />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Quality tiers" note="List allowed source and resolution pairs from lowest to highest quality.">
      <Rows field={tiers} label="Quality tiers" singular="Tier" empty="At least one quality tier is required." ordered
        create={() => ({ source: "web", resolution: 1080 })}>
        {(tier, change, index) => <>
          <SelectField label="Source" help="Release type for this tier."
            field={rowField(form, tiers.path, index, "source", tier.source, source => change({ ...tier, source }))} options={tierSources} />
          <SelectField label="Resolution" help="Video resolution for this tier."
            field={rowField(form, tiers.path, index, "resolution", tier.resolution, resolution => change({ ...tier, resolution }))} options={tierResolutions} />
        </>}
      </Rows>
    </SettingsGroup>
  </>;
}

export function VideoSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), movies = root.group("movies"), tv = root.group("tv"), shadow = movies.group("shadow");
  return <>
    <SettingsGroup title="Plex publishing">
      <Grid>
        <EndpointField label="Plex URL" field={movies.field("plexUrl")} help="Server address used to scan and verify movies and TV in Plex." />
        <SecretField label="Plex library token" field={movies.field("plexToken")} help="Authorizes library operations, not Plex sign-in." />
        <TextField label="Movie Plex section ID" field={movies.field("plexSectionId")} help="Numeric ID of the Plex library where movies are published." />
        <TextField label="TV Plex section ID" field={tv.field("plexSectionId")} help="Numeric ID of the Plex library where TV is published." />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Movie and TV metadata">
      <SecretField label="TMDB access token" field={movies.field("tmdbAccessToken")} help="Authorizes movie and TV catalog lookups." />
    </SettingsGroup>
    <SettingsGroup title="Radarr observation" note="Read Radarr history for comparison and existing-library previews.">
      <Grid>
        <EndpointField label="Radarr URL" field={shadow.field("radarrUrl")} help="Address of the Radarr instance to observe." />
        <SecretField label="Radarr API key" field={shadow.field("radarrApiKey")} help="Authorizes reading Radarr imports and library records." />
        <NumberField label="Radarr poll interval (minutes)" field={shadow.field("pollIntervalMinutes")} help="How often Podible checks Radarr import history." />
        <NumberField label="Radarr lookback (days)" field={shadow.field("lookbackDays")} help="How far back each check reads import history." />
        <NumberField label="Maximum Radarr cases per sync" field={shadow.field("maxCasesPerSync")} help="Maximum new comparison cases to queue per check." />
        <CheckField label="Enable Radarr observation" field={shadow.field("enabled")} help="Compare Radarr imports without changing anything in Radarr." />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Sonarr">
      <Grid>
        <EndpointField label="Sonarr URL" field={tv.field("sonarrUrl")} help="Address used to read series, episodes, and import history." />
        <SecretField label="Sonarr API key" field={tv.field("sonarrApiKey")} help="Authorizes reading data from Sonarr." />
        <NumberField label="Sonarr poll interval (minutes)" field={tv.field("pollIntervalMinutes")} help="How often Podible checks TV requests and Sonarr imports." />
        <NumberField label="Sonarr lookback (days)" field={tv.field("lookbackDays")} help="How far back each check reads import history." />
        <NumberField label="Maximum Sonarr cases per sync" field={tv.field("maxCasesPerSync")} help="Maximum new comparison cases to queue per check." />
      </Grid>
    </SettingsGroup>
  </>;
}

export function PlexSettings({ form }: { form: SettingsForm }) {
  const auth = fields(form).group("auth"), plex = auth.group("plex");
  return <>
    <SettingsGroup title="Sign-in and app redirects">
      <Grid>
        <TextField label="Plex product name" field={plex.field("productName")} help="App name shown during Plex sign-in." />
        <ListField label="Allowed app redirect URIs" field={auth.field("appRedirectURIs")} help="Where Plex sign-in can return users to an app. Enter one exact URI per line." />
      </Grid>
    </SettingsGroup>
    <details className="settings-advanced">
      <summary>Advanced Plex identity settings</summary>
      <p className="settings-note">Changing this identity can affect sign-in. Prefer the verified server picker above.</p>
      <Grid>
        <SecretField label="Plex owner token" field={plex.field("ownerToken")} help="Lets Podible look up servers and shared access for the owner account." />
        <TextField label="Plex owner client identifier" field={plex.field("ownerClientIdentifier")} help="Client ID sent to Plex with the owner token." />
        <TextField label="Plex access server machine ID" field={plex.field("machineId")} help="Identifier of the server whose shared users can sign in." />
      </Grid>
    </details>
  </>;
}

export function StorageSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), retention = root.group("retention");
  return <>
    <SettingsGroup title="Library location" note="Books, movies and TV use one root. Plex scans the published views, not the canonical files.">
      <TextField label="Library root directory" field={root.field("storageRoot")} placeholder="/media/podible"
        help="Parent directory for managed media. Moving an existing library requires a migration." />
      {!form.saved.storageRoot && <p className="settings-note">This library still uses separate storage locations. Existing files and Plex views stay where they are until migrated.</p>}
      <dl className="settings-facts">
        <div><dt>Movies in Plex</dt><dd>{form.saved.movies.plexViewPath || "Not configured"}</dd></div>
        <div><dt>TV in Plex</dt><dd>{form.saved.tv.plexViewPath || "Not configured"}</dd></div>
      </dl>
    </SettingsGroup>
    <SettingsGroup title="Retention policy" note="When free space runs low, Podible can remove eligible movie and TV library copies.">
      <NumberField label="Minimum free space (GiB)" field={retention.field("minimumFreeBytes")} scale={GiB} min={0} help="The free-space threshold for automatic cleanup." />
    </SettingsGroup>
  </>;
}

export function NotificationSettings({ form }: { form: SettingsForm }) {
  const notifications = fields(form).group("notifications"), pushover = notifications.group("pushover");
  const delivery = notifications.field("delivery");
  return <>
    <SettingsGroup title="External delivery">
      <SelectField<"dry-run" | "live"> label="Delivery mode" field={{ ...delivery, value: delivery.value ?? "dry-run", persisted: delivery.persisted ?? "dry-run" }}
        help="Dry run logs alerts without sending them. In-app notifications still work."
        options={[["dry-run", "Dry run"], ["live", "Live"]]} />
    </SettingsGroup>
    <SettingsGroup title="Pushover" note="Send administrator alerts to Pushover.">
    <Grid>
      <CheckField label="Enable Pushover notifications" field={pushover.field("enabled")} help="Send administrator alerts when delivery mode is Live." />
      <SecretField label="Pushover API token" field={pushover.field("apiToken")} help="Application token that authorizes sending alerts." />
      <SecretField label="Pushover user key" field={pushover.field("userKey")} help="Account key that determines who receives the alerts." />
    </Grid>
    </SettingsGroup>
  </>;
}
