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

function Frame({ field, label, help, children }: {
  field: Pick<Field<unknown>, "path" | "issue">; label: string; help?: string;
  children(props: { id: string; "aria-describedby"?: string; "aria-invalid"?: true }): ReactNode;
}) {
  const id = fieldId(field);
  return <div className="settings-field">
    <label htmlFor={id}>{label}</label>
    {children({ id, "aria-describedby": [help && `${id}-help`, field.issue && `${id}-error`].filter(Boolean).join(" ") || undefined,
      "aria-invalid": field.issue ? true : undefined })}
    {help && <small id={`${id}-help`}>{help}</small>}
    {field.issue && <small id={`${id}-error`} className="settings-field-error">{field.issue}</small>}
  </div>;
}

export function TextField({ field, label, help, placeholder, nullable }: {
  field: Field<string | null | undefined>; label: string; help?: string; placeholder?: string; nullable?: boolean;
}) {
  return <Frame {...{ field, label, help }}>{props => <input {...props} type="text" spellCheck={false} autoComplete="off"
    placeholder={placeholder} value={field.value ?? ""} onChange={event => field.set(event.target.value || (nullable ? null : ""), label)} />}</Frame>;
}

function SecretField({ field, label, help }: { field: Field<string | undefined>; label: string; help?: string }) {
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

function EndpointField({ field, label, placeholder, storedUrls }: {
  field: Field<string>; label: string; placeholder?: string; storedUrls?: ReadonlySet<string>;
}) {
  return (hasUrlCredentials(field.persisted) || storedUrls?.has(field.value))
    ? <SecretField field={field} label={label} help="The saved URL contains credentials and is hidden." />
    : <TextField {...{ field, label, placeholder }} />;
}

const GiB = 1024 ** 3;

function NumberField({ field, label, help, nullable, scale = 1, min }: {
  field: Field<number | null>; label: string; help?: string; nullable?: boolean; scale?: number; min?: number;
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

function CheckField({ field, label, help }: { field: Field<boolean>; label: string; help?: string }) {
  const id = fieldId(field);
  return <label className="settings-check">
    <input type="checkbox" checked={field.value} aria-describedby={help ? `${id}-help` : undefined}
      onChange={event => field.set(event.target.checked, label)} />
    <span>{label}{help && <small id={`${id}-help`}>{help}</small>}</span>
  </label>;
}

function SelectField<T extends string | number>({ field, label, options }: {
  field: Field<T>; label: string; options: readonly (readonly [T, string])[];
}) {
  return <Frame {...{ field, label }}>{props => <select {...props} value={field.value}
    onChange={event => {
      const option = options.find(([value]) => String(value) === event.target.value);
      if (option) field.set(option[0], label);
    }}>
    {options.map(([value, name]) => <option key={value} value={value}>{name}</option>)}
  </select>}</Frame>;
}

function ListField({ field, label, help, parse }: {
  field: Field<string[]>; label: string; help?: string; parse?: undefined;
} | {
  field: Field<number[]>; label: string; help?: string; parse: (line: string) => number;
}) {
  const [typing, setTyping] = useState<{ value: (string | number)[]; input: string }>();
  const input = field.input ?? (typing && typing.value.length === field.value.length &&
    field.value.every((value, index) => Object.is(value, typing.value[index])) ? typing.input : undefined);
  return <Frame {...{ field, label, help }}>{props => <textarea {...props} rows={3} spellCheck={false} autoComplete="off"
    value={input ?? field.value.join("\n")}
    onChange={event => {
      const input = event.target.value, lines = input.split("\n").map(line => line.trim()).filter(Boolean);
      if (parse) {
        const value = lines.map(parse);
        setTyping({ value, input });
        field.set(value, label, input);
      } else {
        setTyping({ value: lines, input });
        field.set(lines, label, input);
      }
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
        <TextField label="Feed title" field={feed.field("title")} />
        <TextField label="Feed author" field={feed.field("author")} />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Edition matching">
      <Grid>
        <TextField label="Edition preference" field={agents.field("editionPreference")} help="Guidance for choosing among book editions." />
      </Grid>
    </SettingsGroup>
  </>;
}

export function AiSettings({ form }: { form: SettingsForm }) {
  const agents = fields(form).group("agents");
  return <SettingsGroup title="OpenAI">
    <Grid>
      <SecretField label="OpenAI API key" field={agents.field("apiKey")} />
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
const formatIndexerMedia = (media: string[]) => media.map(kind => indexerMedia[kind]).join(", ");

export function IndexerSettings({ form, discovery, loading }: { form: SettingsForm; discovery?: DiscoveredIndexers; loading: boolean }) {
  const root = fields(form), prowlarr = root.group("prowlarr"), sources = root.field("torznab");
  const original = form.draft.torznab?.original as TorznabSource[] | undefined;
  const protectedUrls = new Set([...(original ?? []), ...form.saved.torznab].map(source => source.baseUrl).filter(hasUrlCredentials));
  return <>
    <SettingsGroup title="Prowlarr" note="Connect the indexer manager used for media searches and feeds.">
      <Grid><EndpointField label="Prowlarr URL" field={prowlarr.field("url")} placeholder="https://prowlarr.example" />
        <SecretField label="Prowlarr API key" field={prowlarr.field("apiKey")} /></Grid>
    </SettingsGroup>
    {form.saved.prowlarr.url && <SettingsGroup title="Discovered indexers">
      {loading ? <p role="status">Loading indexers...</p> : discovery?.state !== "available"
        ? <p>Prowlarr discovery is unavailable. Check the saved URL and API key.</p>
        : discovery.indexers.length === 0 ? <p>No enabled torrent indexers were discovered.</p>
        : <ul className="settings-indexer-list">{discovery.indexers.map(indexer => <li key={indexer.id}>
          <strong>{indexer.name}</strong>
          {indexer.retryAt !== null && <span> · Cooling down</span>}
          {indexer.search.length > 0 && <small>Search: {formatIndexerMedia(indexer.search)}</small>}
          {indexer.feeds.length > 0 && <small>Feeds: {formatIndexerMedia(indexer.feeds)}</small>}
        </li>)}</ul>}
    </SettingsGroup>}
    <SettingsGroup title="Torznab sources" note={form.saved.prowlarr.url
      ? "Prowlarr is active. Manual sources are saved but not queried."
      : "Configure direct sources to use without Prowlarr."}>
      <Rows<TorznabSource> field={sources} label="Torznab sources" singular="Torznab source" empty="No direct Torznab sources configured."
        create={() => ({ name: "", baseUrl: "" })}>
        {(source, change, index) => <>
          {source.baseUrl === form.saved.torznab[index]?.baseUrl && discovery?.duplicates.find(row => row.index === index) &&
            <p className="settings-note">Already discovered through Prowlarr; not queried twice.</p>}
          <TextField label="Source name" field={rowField(form, sources.path, index, "name", source.name, name => change({ ...source, name }))} />
          <EndpointField label="Torznab URL" storedUrls={protectedUrls}
            field={rowField(form, sources.path, index, "baseUrl", source.baseUrl, baseUrl => change({ ...source, baseUrl }))} />
          <SecretField label="Torznab API key" field={rowField(form, sources.path, index, "apiKey", source.apiKey, apiKey => change({ ...source, apiKey }))} />
          <TextField label="Audiobook categories" field={rowField(form, sources.path, index, "categories.audio", source.categories?.audio,
            audio => change({ ...source, categories: { ...source.categories, audio } }))} help="Category IDs, separated by commas." />
          <TextField label="Ebook categories" field={rowField(form, sources.path, index, "categories.ebook", source.categories?.ebook,
            ebook => change({ ...source, categories: { ...source.categories, ebook } }))} help="Category IDs, separated by commas." />
        </>}
      </Rows>
    </SettingsGroup>
  </>;
}

export function DownloadSettings({ form }: { form: SettingsForm }) {
  const root = fields(form);
  return <>
    <SettingsGroup title="Download recovery">
      <Grid>
        <NumberField label="Stalled torrent threshold (minutes)" field={root.group("recovery").field("stalledTorrentMinutes")} min={0}
          help="Set to 0 to retry as soon as a stall is detected." />
      </Grid>
    </SettingsGroup>
  </>;
}

const tierSources = [["web", "WEB"], ["bluray", "Blu-ray"], ["hdtv", "HDTV"], ["dvd", "DVD"], ["cam", "CAM"]] as const;
const tierResolutions = [[480, "480p"], [720, "720p"], [1080, "1080p"], [2160, "2160p"]] as const;

export function MovieSettings({ form }: { form: SettingsForm }) {
  const movies = fields(form).group("movies"), seerr = movies.group("seerr"), shadow = movies.group("shadow");
  const quality = movies.group("qualityPolicy"), tiers = quality.field("tiers");
  return <>
    <SettingsGroup title="Movie library and Plex publishing">
      <Grid>
        <EndpointField label="Plex URL" field={movies.field("plexUrl")} />
        <SecretField label="Plex library token" field={movies.field("plexToken")} help="Used for library operations, separate from the Plex sign-in token." />
        <TextField label="Movie Plex section ID" field={movies.field("plexSectionId")} />
        <SecretField label="TMDB access token" field={movies.field("tmdbAccessToken")} />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Seerr" note="The same Seerr connection supplies movie and TV requests.">
      <Grid>
        <EndpointField label="Seerr URL" field={seerr.field("url")} />
        <SecretField label="Seerr API key" field={seerr.field("apiKey")} />
        <NumberField label="Seerr poll interval (minutes)" field={seerr.field("pollIntervalMinutes")} />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Acquisition">
      <Grid>
        <NumberField label="Download group ID" field={movies.field("downloadGroupId")} nullable help="Leave blank to use the default group." />
        <NumberField label="Maximum movie release size (GiB)" field={movies.field("maximumReleaseBytes")} scale={GiB} min={1} nullable help="Leave blank for no size cap." />
        <SelectField label="Cross-seed preference" field={movies.field("crossSeedPreference")} options={[["off", "Off"], ["tie-break", "Prefer reuse as a tie-breaker"]]} />
        <CheckField label="Automatically upgrade movies" field={movies.field("automaticUpgrades")} />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Release quality" note="Set which releases are eligible and how Podible ranks upgrades. TV also uses this policy.">
      <Grid>
        <TextField label="Required cut" field={quality.field("cut")} nullable help="Leave blank to allow any cut." />
        <NumberField label="Minimum preference gain" field={quality.field("minimumPreferenceGain")} help="Minimum score improvement for a same-tier upgrade." />
        <ListField label="Audio languages" field={quality.field("audioLanguages")} help="One language code per line, such as en. Leave blank to allow any language." />
        <ListField label="Allowed codecs" field={quality.field("allowedCodecs")} help="One codec per line, such as h264, hevc or av1." />
        <ListField label="Preferred codecs" field={quality.field("preferredCodecs")} help="One codec per line, most preferred first." />
        <div className="settings-checks">
          <CheckField label="Allow HDR" field={quality.field("allowHdr")} />
          <CheckField label="Prefer proper and repack releases" field={quality.field("preferProper")} />
        </div>
      </Grid>
      <h4>Quality tiers</h4>
      <p className="settings-note">List allowed source and resolution pairs from lowest to highest quality.</p>
      <Rows field={tiers} label="Quality tiers" singular="Tier" empty="At least one quality tier is required." ordered
        create={() => ({ source: "web", resolution: 1080 })}>
        {(tier, change, index) => <>
          <SelectField label="Source" field={rowField(form, tiers.path, index, "source", tier.source, source => change({ ...tier, source }))} options={tierSources} />
          <SelectField label="Resolution" field={rowField(form, tiers.path, index, "resolution", tier.resolution, resolution => change({ ...tier, resolution }))} options={tierResolutions} />
        </>}
      </Rows>
    </SettingsGroup>
    <SettingsGroup title="Radarr observation" note="Read Radarr history for comparison and existing-library previews.">
      <Grid>
        <EndpointField label="Radarr URL" field={shadow.field("radarrUrl")} />
        <SecretField label="Radarr API key" field={shadow.field("radarrApiKey")} />
        <NumberField label="Radarr poll interval (minutes)" field={shadow.field("pollIntervalMinutes")} />
        <NumberField label="Radarr lookback (days)" field={shadow.field("lookbackDays")} />
        <NumberField label="Maximum Radarr cases per sync" field={shadow.field("maxCasesPerSync")} />
        <CheckField label="Enable Radarr observation" field={shadow.field("enabled")} />
      </Grid>
    </SettingsGroup>
  </>;
}

export function TvSettings({ form, openMovies }: { form: SettingsForm; openMovies(): void }) {
  const tv = fields(form).group("tv");
  return <>
    <SettingsGroup title="TV library and Plex publishing">
      <Grid>
        <TextField label="TV Plex section ID" field={tv.field("plexSectionId")} />
        <NumberField label="Maximum TV release size (GiB)" field={tv.field("maximumReleaseBytes")} scale={GiB} min={1} nullable help="Leave blank for no size cap." />
      </Grid>
      <p className="settings-note">TV shares the Seerr connection and release quality policy in <button type="button" className="settings-text-button" onClick={openMovies}>Movies</button>.</p>
    </SettingsGroup>
    <SettingsGroup title="Sonarr">
      <Grid>
        <EndpointField label="Sonarr URL" field={tv.field("sonarrUrl")} />
        <SecretField label="Sonarr API key" field={tv.field("sonarrApiKey")} />
        <NumberField label="Sonarr poll interval (minutes)" field={tv.field("pollIntervalMinutes")} />
        <NumberField label="Sonarr lookback (days)" field={tv.field("lookbackDays")} />
        <NumberField label="Maximum Sonarr cases per sync" field={tv.field("maxCasesPerSync")} />
      </Grid>
    </SettingsGroup>
  </>;
}

export function PlexSettings({ form }: { form: SettingsForm }) {
  const auth = fields(form).group("auth"), plex = auth.group("plex");
  return <>
    <SettingsGroup title="Sign-in and app redirects">
      <Grid>
        <TextField label="Plex product name" field={plex.field("productName")} />
        <ListField label="Allowed app redirect URIs" field={auth.field("appRedirectURIs")} help="One exact URI per line." />
      </Grid>
    </SettingsGroup>
    <details className="settings-advanced">
      <summary>Advanced Plex identity settings</summary>
      <p className="settings-note">Changing this identity can affect sign-in. Prefer the verified server picker above.</p>
      <Grid>
        <SecretField label="Plex owner token" field={plex.field("ownerToken")} />
        <TextField label="Plex owner client identifier" field={plex.field("ownerClientIdentifier")} />
        <TextField label="Plex access server machine ID" field={plex.field("machineId")} />
      </Grid>
    </details>
  </>;
}

export function StorageSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), retention = root.group("retention");
  const id = (line: string) => numericSetting(line) ?? NaN;
  return <>
    <SettingsGroup title="Library location" note="Books, movies and TV use one root. Plex scans the published views, not the canonical files.">
      <TextField label="Library root directory" field={root.field("storageRoot")} placeholder="/media/podible"
        help="Changing the root of an existing library requires a storage migration." />
      {!form.saved.storageRoot && <p className="settings-note">Existing locations stay in use until the library is migrated or a root is set before adding content.</p>}
      <dl className="settings-facts">
        <div><dt>Movies in Plex</dt><dd>{form.saved.movies.plexViewPath || "Not configured"}</dd></div>
        <div><dt>TV in Plex</dt><dd>{form.saved.tv.plexViewPath || "Not configured"}</dd></div>
      </dl>
    </SettingsGroup>
    <SettingsGroup title="Retention policy" note="When free space runs low, Podible can remove eligible movie and TV library copies. Protected titles are excluded.">
    <Grid>
      <NumberField label="Minimum free space (GiB)" field={retention.field("minimumFreeBytes")} scale={GiB} min={0} help="The free-space threshold for automatic cleanup." />
      <ListField label="Protected movie target IDs" field={retention.field("protectedMovieTargetIds")} parse={id} help="One Podible target ID per line." />
      <ListField label="Protected TV target IDs" field={retention.field("protectedTvTargetIds")} parse={id} help="One Podible target ID per line." />
    </Grid>
    </SettingsGroup>
  </>;
}

export function NotificationSettings({ form }: { form: SettingsForm }) {
  const notifications = fields(form).group("notifications"), pushover = notifications.group("pushover");
  const delivery = notifications.field("delivery");
  return <>
    <SettingsGroup title="External delivery">
      <SelectField<"dry-run" | "live"> label="Delivery mode" field={{ ...delivery, value: delivery.value ?? "dry-run", persisted: delivery.persisted ?? "dry-run" }}
        options={[["dry-run", "Dry run"], ["live", "Live"]]} />
      <p className="settings-note">Dry run logs skipped delivery without contacting Pushover. In-app notifications still work.</p>
    </SettingsGroup>
    <SettingsGroup title="Pushover" note="Send administrator alerts to Pushover.">
    <Grid>
      <CheckField label="Enable Pushover notifications" field={pushover.field("enabled")} />
    </Grid>
    <Grid>
      <SecretField label="Pushover API token" field={pushover.field("apiToken")} />
      <SecretField label="Pushover user key" field={pushover.field("userKey")} />
    </Grid>
    </SettingsGroup>
  </>;
}
