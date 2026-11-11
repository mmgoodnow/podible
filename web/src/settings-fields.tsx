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
        placeholder={field.value ? "Enter a replacement" : "Enter a value"}
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

function NumberField({ field, label, help, nullable, decimal, min }: {
  field: Field<number | null>; label: string; help?: string; nullable?: boolean; decimal?: boolean; min?: number;
}) {
  const [typing, setTyping] = useState<{ value: number | null; input: string }>();
  const input = field.input ?? (typing && Object.is(typing.value, field.value) ? typing.input : undefined);
  return <Frame {...{ field, label, help }}>{props => <input {...props} type="text" inputMode={decimal ? "decimal" : "numeric"}
    autoComplete="off" placeholder={nullable ? "Not set" : undefined} value={input ?? field.value ?? ""}
    onChange={event => {
      const input = event.target.value, value = numericSetting(input, nullable);
      const valid = value === null || ((min === undefined || value >= min) && (decimal || Number.isSafeInteger(value)));
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

function PathMappings({ form, field }: { form: SettingsForm; field: Field<{ remoteRoot: string; localRoot: string }[]> }) {
  return <Rows field={field} label="Path mappings" singular="Path mapping" empty="No path mappings configured."
    create={() => ({ remoteRoot: "", localRoot: "" })}>
    {(row, change, index) => <>
      <TextField label="Remote root" field={rowField(form, field.path, index, "remoteRoot", row.remoteRoot, remoteRoot => change({ ...row, remoteRoot }))} />
      <TextField label="Local root" field={rowField(form, field.path, index, "localRoot", row.localRoot, localRoot => change({ ...row, localRoot }))} />
    </>}
  </Rows>;
}

export function BookSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), feed = root.group("feed"), agents = root.group("agents");
  return <>
    <SettingsGroup title="Book library and feed">
      <Grid>
        <TextField label="Book library root" field={root.field("libraryRoot")} help="Where Podible stores its managed ebooks and audiobooks." />
        <TextField label="Feed title" field={feed.field("title")} />
        <TextField label="Feed author" field={feed.field("author")} />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Edition matching" note="The optional OpenAI Responses integration helps evaluate book editions.">
      <Grid>
        <SecretField label="OpenAI API key" field={agents.field("apiKey")} />
        <TextField label="Edition preference" field={agents.field("editionPreference")} help="Guidance for choosing among book editions." />
        <NumberField label="Low-confidence threshold" field={agents.field("lowConfidenceThreshold")} decimal help="Matches below this score can be escalated to the agent." />
        <NumberField label="Agent timeout (ms)" field={agents.field("timeoutMs")} min={1000} help="At least 1,000 milliseconds." />
      </Grid>
    </SettingsGroup>
  </>;
}

export function IndexerSettings({ form }: { form: SettingsForm }) {
  const root = fields(form), prowlarr = root.group("prowlarr"), sources = root.field("torznab");
  const original = form.draft.torznab?.original as TorznabSource[] | undefined;
  const protectedUrls = new Set([...(original ?? []), ...form.saved.torznab].map(source => source.baseUrl).filter(hasUrlCredentials));
  return <>
    <SettingsGroup title="Prowlarr" note="Connect the indexer manager used for media searches and feeds.">
      <Grid><EndpointField label="Prowlarr URL" field={prowlarr.field("url")} placeholder="https://prowlarr.example" />
        <SecretField label="Prowlarr API key" field={prowlarr.field("apiKey")} /></Grid>
    </SettingsGroup>
    <SettingsGroup title="Torznab sources" note="Configure direct sources and category IDs for book searches.">
      <Rows<TorznabSource> field={sources} label="Torznab sources" singular="Torznab source" empty="No direct Torznab sources configured."
        create={() => ({ name: "", baseUrl: "" })}>
        {(source, change, index) => <>
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
  const root = fields(form), rtorrent = root.group("rtorrent");
  return <>
    <SettingsGroup title="rTorrent" note="Connection and path settings for the legacy HTTP/XML-RPC torrent client.">
      <Grid>
        <EndpointField label="rTorrent RPC URL" field={rtorrent.field("url")} placeholder="http://127.0.0.1/RPC2" />
        <TextField label="Download path" field={rtorrent.field("downloadPath")} />
        <TextField label="Username" field={rtorrent.field("username")} />
        <SecretField label="rTorrent password" field={rtorrent.field("password")} />
      </Grid>
    </SettingsGroup>
    <SettingsGroup title="Polling and recovery">
      <Grid>
        <NumberField label="Torrent poll interval (ms)" field={root.group("polling").field("rtorrentMs")} />
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
        <TextField label="Movie managed root" field={movies.field("managedRoot")} />
        <TextField label="Movie Plex view path" field={movies.field("plexViewPath")} />
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
        <NumberField label="Maximum movie release size (bytes)" field={movies.field("maximumReleaseBytes")} nullable help="Leave blank for no size cap." />
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
      <h4>Radarr path mappings</h4>
      <p className="settings-note">Map paths reported by Radarr to paths visible to Podible.</p>
      <PathMappings form={form} field={shadow.field("pathMappings")} />
    </SettingsGroup>
  </>;
}

export function TvSettings({ form, openMovies }: { form: SettingsForm; openMovies(): void }) {
  const tv = fields(form).group("tv");
  return <>
    <SettingsGroup title="TV library and Plex publishing">
      <Grid>
        <TextField label="TV managed root" field={tv.field("managedRoot")} />
        <TextField label="TV Plex view path" field={tv.field("plexViewPath")} />
        <TextField label="TV Plex section ID" field={tv.field("plexSectionId")} />
        <NumberField label="Maximum TV release size (bytes)" field={tv.field("maximumReleaseBytes")} nullable help="Leave blank for no size cap." />
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
      <h4>Sonarr path mappings</h4>
      <p className="settings-note">Map paths reported by Sonarr to paths visible to Podible.</p>
      <PathMappings form={form} field={tv.field("pathMappings")} />
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
  const retention = fields(form).group("retention");
  const id = (line: string) => numericSetting(line) ?? NaN;
  return <SettingsGroup title="Retention policy" note="When free space runs low, Podible can remove eligible movie and TV library copies. Protected titles are excluded.">
    <Grid>
      <NumberField label="Minimum free space (bytes)" field={retention.field("minimumFreeBytes")} help="The free-space threshold for automatic cleanup. 1 GiB = 1,073,741,824 bytes." />
      <ListField label="Protected movie target IDs" field={retention.field("protectedMovieTargetIds")} parse={id} help="One Podible target ID per line." />
      <ListField label="Protected TV target IDs" field={retention.field("protectedTvTargetIds")} parse={id} help="One Podible target ID per line." />
    </Grid>
  </SettingsGroup>;
}

export function NotificationSettings({ form }: { form: SettingsForm }) {
  const pushover = fields(form).group("notifications").group("pushover");
  return <SettingsGroup title="Pushover" note="Send notifications through your Pushover application.">
    <Grid>
      <CheckField label="Enable Pushover notifications" field={pushover.field("enabled")} />
    </Grid>
    <Grid>
      <SecretField label="Pushover API token" field={pushover.field("apiToken")} />
      <SecretField label="Pushover user key" field={pushover.field("userKey")} />
    </Grid>
  </SettingsGroup>;
}
