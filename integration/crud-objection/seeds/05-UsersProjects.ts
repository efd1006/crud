import * as Knex from 'knex';
import { Model } from 'objection';

export async function seed(knex: Knex): Promise<any> {
  Model.knex(knex);
}
