import { fetch } from 'undici';
import { CookieJar } from 'tough-cookie';
import * as cheerio from 'cheerio';
import { randomBytes } from 'node:crypto';

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const BASE_URL = 'https://rejestrcheb.mrit.gov.pl';
const DWR_URL = `${BASE_URL}/bgk-sr-portlet/dwr/call/plaincall/Module.request.dwr`;

export interface DwrModuleConfig {
  moduleName: string;
  portalPath: string;
  conditionComparators: string[];
}

export const INZYNIEROWIE_CFG: DwrModuleConfig = {
  moduleName: 'OpublikowanyUprawniony',
  portalPath: '/rejestr-uprawnionych',
  conditionComparators: [
    'contains_comparator',
    'eq',
    'eq',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
  ],
};

export const SWIADECTWA_CFG: DwrModuleConfig = {
  moduleName: 'ZatwierdzoneSwiadectwoEnergetyczneWykaz',
  portalPath: '/wykaz-swiadectw-charakterystyki-energetycznej-budynkow',
  conditionComparators: [
    'contains_comparator',
    'eq',
    'eq',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
    'contains_comparator',
    'eq',
    'eq',
    'eq',
    'eq',
    'eq',
  ],
};

export class MritDwrClient {
  private jar = new CookieJar();
  private scriptSessionId = randomBytes(16).toString('hex').toUpperCase();
  private batchId = 1;

  constructor(private cfg: DwrModuleConfig) {}

  async initSession(): Promise<void> {
    await this.httpFetch(`${BASE_URL}${this.cfg.portalPath}`, { method: 'GET' });
    await this.dwrCall('List.setPageRowCount', 'rowCount%3D100%2Ccollection%3D');
    await sleep(3000);
  }

  async goToPage(pageNum: number): Promise<string> {
    return this.dwrCall('List.goPage', `page%3D${pageNum}`);
  }

  private async dwrCall(action: string, argument: string): Promise<string> {
    const payload = this.buildPayload(action, argument);
    this.batchId++;

    const res = await this.httpFetch(DWR_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'text/plain',
        'Referer': `${BASE_URL}${this.cfg.portalPath}`,
      },
      body: payload,
    });

    if (!res.ok) {
      throw new Error(`DWR call failed: HTTP ${res.status}`);
    }

    const text = await res.text();
    return decodeUnicodeEscapes(text);
  }

  private buildPayload(action: string, argument: string): string {
    const mod = this.cfg.moduleName;
    const comparators = this.cfg.conditionComparators;

    const lines: string[] = [
      'callCount=1',
      `page=${this.cfg.portalPath}`,
      'httpSessionId=',
      `scriptSessionId=${this.scriptSessionId}`,
      'c0-scriptName=Module',
      'c0-methodName=request',
      'c0-id=0',
      'c0-param0=boolean:false',
      'c0-param1=boolean:false',
      'c0-param2=string:bgk-sr',
      `c0-param3=string:${mod}`,
      'c0-param4=string:%26p_l_id%3D438601%26p_v_l_s_g_id%3D0',
      `c0-e1=string:${action}`,
      `c0-e2=string:${argument}`,
      'c0-e3=string:undefined',
      'c0-e4=string:undefined_',
      'c0-e5=string:bgk-sr',
      `c0-e6=string:${mod}`,
      'c0-e7=string:',
      'c0-e8=string:',
      'c0-e9=string:',
      'c0-e10=string:false',
      'c0-e11=string:',
      'c0-e12=string:',
      'c0-e13=string:%2Fbgk-sr-portlet%2Fxava%2Fimages%2F',
      'c0-e14=boolean:false',
      'c0-e15=string:',
      'c0-e16=string:',
    ];

    // condition groups: for each of N groups, emit 3 fields (comparator, value, valueTo)
    // starting at c0-e17
    let fieldIdx = 17;
    for (let i = 0; i < comparators.length; i++) {
      lines.push(`c0-e${fieldIdx}=string:${comparators[i]}`);
      lines.push(`c0-e${fieldIdx + 1}=string:`);
      lines.push(`c0-e${fieldIdx + 2}=string:`);
      fieldIdx += 3;
    }

    // tail fields: always at c0-e62..c0-e66 regardless of condition count
    lines.push(
      'c0-e62=boolean:false',
      'c0-e63=string:',
      'c0-e64=string:',
      'c0-e65=string:100',
      'c0-e66=string:',
    );

    // param5: Object_Object mapping XAVA field names → c0-eN references
    const objectMap = this.buildParam5(mod, comparators.length);
    lines.push(`c0-param5=Object_Object:${objectMap}`);

    lines.push(
      'c0-param6=Object_Object:{}',
      'c0-param7=Array:[]',
      'c0-param8=null:null',
      `batchId=${this.batchId}`,
    );

    return lines.join('\n') + '\n';
  }

  private buildParam5(mod: string, conditionCount: number): string {
    const prefix = `ox_bgk-sr_${mod}__`;
    const parts: string[] = [
      `${prefix}xava_action:reference:c0-e1`,
      `${prefix}xava_action_argv:reference:c0-e2`,
      `${prefix}xava_action_range:reference:c0-e3`,
      `${prefix}xava_action_already_processed:reference:c0-e4`,
      `${prefix}xava_action_application:reference:c0-e5`,
      `${prefix}xava_action_module:reference:c0-e6`,
      `${prefix}xava_changed_property:reference:c0-e7`,
      `${prefix}xava_current_focus:reference:c0-e8`,
      `${prefix}xava_previous_focus:reference:c0-e9`,
      `${prefix}xava_focus_forward:reference:c0-e10`,
      `${prefix}xava_focus_property_id:reference:c0-e11`,
      `xava_listwykazTab_filter_visible:reference:c0-e12`,
      `xava_image_filter_prefix:reference:c0-e13`,
      `${prefix}xava_selected_all:reference:c0-e14`,
      `${prefix}action___List___orderBy:reference:c0-e15`,
      `${prefix}action___List___filter:reference:c0-e16`,
    ];

    let fieldIdx = 17;
    for (let i = 0; i < conditionCount; i++) {
      parts.push(`${prefix}conditionComparator___${i}:reference:c0-e${fieldIdx}`);
      parts.push(`${prefix}conditionValue___${i}:reference:c0-e${fieldIdx + 1}`);
      parts.push(`${prefix}conditionValueTo___${i}:reference:c0-e${fieldIdx + 2}`);
      fieldIdx += 3;
    }

    parts.push(
      `${prefix}xava_selected:reference:c0-e62`,
      `${prefix}action___List___goPage:reference:c0-e63`,
      `${prefix}action___List___goNextPage:reference:c0-e64`,
      `${prefix}list_rowCount:reference:c0-e65`,
      `${prefix}action___List___hideRows:reference:c0-e66`,
    );

    return `{${parts.join(', ')}}`;
  }

  private async httpFetch(
    url: string,
    opts: Parameters<typeof fetch>[1] = {},
  ): ReturnType<typeof fetch> {
    const cookieHeader = await this.jar.getCookieString(url);
    const headers: Record<string, string> = {
      'User-Agent': USER_AGENT,
      ...((opts.headers as Record<string, string>) ?? {}),
    };
    if (cookieHeader) headers['Cookie'] = cookieHeader;

    const res = await fetch(url, { ...opts, headers });

    const setCookies = res.headers.getSetCookie();
    for (const c of setCookies) {
      try {
        await this.jar.setCookie(c, url);
      } catch {
        // Ignore malformed cookies from the old portal
      }
    }

    return res;
  }
}

export interface ParsedTable {
  headers: string[];
  rows: string[][];
}

export function parseDwrTable(response: string, minTableLength = 1000): ParsedTable {
  const tableRegex = /<table[^>]*>[\s\S]*?<\/table>/g;
  let mainTable = '';
  for (const match of response.matchAll(tableRegex)) {
    if (match[0].length > minTableLength) {
      mainTable = match[0]
        .replace(/\\r/g, '')
        .replace(/\\n/g, '')
        .replace(/\\t/g, '')
        .replace(/\\"/g, '"')
        .replace(/\\\//g, '/');
      break;
    }
  }

  if (!mainTable) return { headers: [], rows: [] };

  const $ = cheerio.load(mainTable);
  const headers = $('th')
    .map((_, el) => cleanText($(el).text()))
    .get();

  const rows: string[][] = [];
  $('tr.portlet-section-body, tr.portlet-section-alternate').each((_, tr) => {
    const cells = $(tr)
      .find('td')
      .map((_, td) => cleanText($(td).text()))
      .get();
    rows.push(cells);
  });

  return { headers, rows };
}

export function buildColumnMap(
  headers: string[],
  mapping: Record<string, (header: string) => boolean>,
): Record<string, number> {
  const result: Record<string, number> = {};
  headers.forEach((header, idx) => {
    const lower = header.toLowerCase();
    for (const [key, predicate] of Object.entries(mapping)) {
      if (result[key] === undefined && predicate(lower)) {
        result[key] = idx;
      }
    }
  });
  return result;
}

function cleanText(text: string): string {
  return text.replace(/\u00A0/g, ' ').replace(/&nbsp;/g, ' ').trim();
}

function decodeUnicodeEscapes(text: string): string {
  return text.replace(/\\u([0-9a-fA-F]{4})/g, (_match, hex: string) =>
    String.fromCharCode(parseInt(hex, 16)),
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
