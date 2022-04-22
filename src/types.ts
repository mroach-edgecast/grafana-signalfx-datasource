import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface SFXQuery extends DataQuery {
  program?: string;
  alias?: string;
  maxDelay?: number;
  minResolution?: number;
  withStreaming: boolean;
}

export const defaultQuery: Partial<SFXQuery> = {
  withStreaming: false,
};

/**
 * These are options configured for each DataSource instance.
 */
export interface SFXDataSourceOptions extends DataSourceJsonData {
  url?: string;
  access?: string;
  accessToken?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface SFXSecureJsonData {
  accessToken?: string;
}
