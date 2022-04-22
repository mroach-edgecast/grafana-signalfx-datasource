import { DataSourceInstanceSettings } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';
import { SFXDataSourceOptions, SFXQuery } from './types';

export class DataSource extends DataSourceWithBackend<SFXQuery, SFXDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<SFXDataSourceOptions>) {
    super(instanceSettings);
  }
}
