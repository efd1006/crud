import {
  CreateManyDto,
  CrudRequest,
  CrudRequestOptions,
  CrudService,
  GetManyDefaultResponse,
  JoinOptions,
  QueryOptions,
} from '@nestjsx/crud';
import {
  ParsedRequestParams,
  QueryFilter,
  QueryJoin,
  QuerySort,
} from '@nestjsx/crud-request';
import {
  hasLength,
  isArrayFull,
  isNil,
  isObject,
  isUndefined,
  objKeys,
} from '@nestjsx/util';
import { Model, ModelClass, QueryBuilder, Relation, transaction } from 'objection';
import { OnModuleInit } from '@nestjs/common';

export class ObjectionCrudService<T extends Model> extends CrudService<T>
  implements OnModuleInit {
  private entityColumns: string[];
  private entityPrimaryColumns: string[];
  private entityColumnsSet: Set<string> = new Set();
  private entityRelationsHash: object = {};

  constructor(public readonly modelClass: ModelClass<T>) {
    super();
  }

  async onModuleInit() {
    await this.fetchTableMetadata(this.modelClass.tableName);
    await this.onInitMapRelations();
    await this.onInitMapEntityColumns();
  }

  async fetchTableMetadata(tableName: string) {
    return await Model.fetchTableMetadata({ table: tableName });
  }

  private get alias(): string {
    return this.modelClass.tableName;
  }

  private toAliased(prop: string) {
    return `${this.alias}.${prop}`;
  }

  private get idColumns(): string[] {
    return [].concat(this.modelClass.idColumn);
  }

  /**
   * Get many
   * @param req
   */
  public async getMany(req: CrudRequest): Promise<GetManyDefaultResponse<T> | T[]> {
    const { parsed, options } = req;
    const builder = this.createBuilder(parsed, options);

    const offsetLimit = decidePagination(parsed, options);
    if (offsetLimit) {
      const [total, data] = await Promise.all([builder.resultSize(), builder]);
      return this.createPageInfo(data, total, offsetLimit.limit, offsetLimit.offset);
    }

    return builder;
  }

  /**
   * Get one
   * @param req
   */
  public async getOne(req: CrudRequest): Promise<T> {
    return this.getOneOrFail(req);
  }

  /**
   * Create one
   * @param req
   * @param dto
   */
  public async createOne(req: CrudRequest, dto: T): Promise<T> {
    const entity = this.prepareEntityBeforeSave(dto, req.parsed.paramsFilter);

    if (!entity) {
      this.throwBadRequestException(`Empty data. Nothing to save.`);
    }

    return this.modelClass.query().insert(entity);
  }

  /**
   * Create many
   * @param req
   * @param dto
   */
  public async createMany(req: CrudRequest, dto: CreateManyDto<T>): Promise<T[]> {
    if (!isObject(dto) || !isArrayFull(dto.bulk)) {
      this.throwBadRequestException(`Empty data. Nothing to save.`);
    }

    const bulk = dto.bulk
      .map((one) => this.prepareEntityBeforeSave(one, req.parsed.paramsFilter))
      .filter((d) => !isUndefined(d));

    if (!hasLength(bulk)) {
      this.throwBadRequestException(`Empty data. Nothing to save.`);
    }

    return transaction(this.modelClass, async (boundedModelClass) => {
      let result = [];

      const chunks = toChunks(bulk, 10000);
      for (const chunk of chunks) {
        result = result.concat(await boundedModelClass.query().insert(chunk));
      }

      return result;
    });
  }

  /**
   * Update one
   * @param req
   * @param dto
   */
  public async updateOne(req: CrudRequest, dto: T): Promise<T> {
    const found = await this.getOneOrFail(req);

    /* istanbul ignore else */
    if (
      hasLength(req.parsed.paramsFilter) &&
      !req.options.routes.updateOneBase.allowParamsOverride
    ) {
      for (const filter of req.parsed.paramsFilter) {
        dto[filter.field] = filter.value;
      }
    }

    return found
      .$query()
      .skipUndefined()
      .patchAndFetch({ ...dto });
  }

  /**
   * Replace one
   * @param req
   * @param dto
   */
  public async replaceOne(req: CrudRequest, dto: T): Promise<T> {
    // TODO: Implement UPSERT
    throw new Error('Not Implemented for Objection.js');
    /* istanbul ignore else */
    // if (
    //   hasLength(req.parsed.paramsFilter) &&
    //   !req.options.routes.replaceOneBase.allowParamsOverride
    // ) {
    //   for (const filter of req.parsed.paramsFilter) {
    //     dto[filter.field] = filter.value;
    //   }
    // }

    // return this.repo.save<any>(dto);
  }

  /**
   * Delete one
   * @param req
   */
  public async deleteOne(req: CrudRequest): Promise<void | T> {
    const found = await this.getOneOrFail(req);
    await found.$query().delete();

    /* istanbul ignore else */
    if (req.options.routes.deleteOneBase.returnDeleted) {
      // set params, because id is undefined
      for (const filter of req.parsed.paramsFilter) {
        found[filter.field] = filter.value;
      }

      return found;
    }
  }

  private async getOneOrFail(req: CrudRequest): Promise<T> {
    const { parsed, options } = req;
    const builder = this.createBuilder(parsed, options);
    const found = await builder.limit(1).first();

    if (!found) {
      this.throwNotFoundException(this.alias);
    }

    return found;
  }

  /**
   * Create Objection.js QueryBuilder
   * @param parsedReq
   * @param options
   * @param many
   */
  public createBuilder(
    parsedReq: ParsedRequestParams,
    // This comes from @Crud({ ... })
    options: CrudRequestOptions,
    many = true,
  ) {
    const builder = this.modelClass.query().skipUndefined();
    const select = this.getSelect(parsedReq, options.query);
    builder.select(select);

    // set mandatory where condition from CrudOptions.query.filter
    if (isArrayFull(options.query.filter)) {
      options.query.filter.forEach((filter) => {
        this.setAndWhere(filter, builder);
      });
    }

    const filters = [...parsedReq.paramsFilter, ...parsedReq.filter];
    const hasFilter = isArrayFull(filters);
    const hasOr = isArrayFull(parsedReq.or);

    if (hasFilter && hasOr) {
      if (filters.length === 1 && parsedReq.or.length === 1) {
        // WHERE :filter OR :or
        builder.andWhere((qb) => {
          this.setOrWhere(filters[0], qb);
          this.setOrWhere(parsedReq.or[0], qb);
        });
      } else if (filters.length === 1) {
        builder.andWhere((qb) => {
          this.setAndWhere(filters[0], qb);
          qb.orWhere((orQb) => {
            parsedReq.or.forEach((filter) => {
              this.setAndWhere(filter, orQb);
            });
          });
        });
      } else if (parsedReq.or.length === 1) {
        builder.andWhere((qb) => {
          this.setAndWhere(parsedReq.or[0], qb);
          qb.orWhere((orQb) => {
            filters.forEach((filter) => {
              this.setAndWhere(filter, orQb);
            });
          });
        });
      } else {
        builder.andWhere((qb) => {
          qb.andWhere((andQb) => {
            filters.forEach((filter) => {
              this.setAndWhere(filter, andQb);
            });
          });
          qb.orWhere((orQb) => {
            parsedReq.or.forEach((filter) => {
              this.setAndWhere(filter, orQb);
            });
          });
        });
      }
    } else if (hasOr) {
      // WHERE :or OR :or OR ...
      builder.andWhere((qb) => {
        parsedReq.or.forEach((filter) => {
          this.setOrWhere(filter, qb);
        });
      });
    } else if (hasFilter) {
      // WHERE :filter AND :filter AND ...
      builder.andWhere((qb) => {
        filters.forEach((filter) => {
          this.setAndWhere(filter, qb);
        });
      });
    }

    // set joins
    const joinOptions = options.query.join || {};
    const allowedJoins = objKeys(joinOptions);

    if (hasLength(allowedJoins)) {
      const eagerJoins: any = {};

      allowedJoins.forEach((allowedJoin) => {
        if (joinOptions[allowedJoin].eager) {
          const cond = parsedReq.join.find(
            (join) => join && join.field === allowedJoin,
          ) || {
            field: allowedJoin,
          };

          this.setJoin(cond, joinOptions, builder);
          eagerJoins[allowedJoin] = true;
        }
      });

      if (isArrayFull(parsedReq.join)) {
        parsedReq.join.forEach((join) => {
          if (!eagerJoins[join.field]) {
            this.setJoin(join, joinOptions, builder);
          }
        });
      }
    }

    /* istanbul ignore else */
    if (many) {
      // set sort (order by)
      const sort = this.getSort(parsedReq, options.query);
      sort.forEach(({ column, order }) => builder.orderBy(column, order));

      const { offset, limit } = getOffsetLimit(parsedReq, options);
      if (isFinite(limit) && !isNil(limit)) {
        builder.limit(limit);
      }

      if (isFinite(offset) && !isNil(offset)) {
        builder.offset(offset);
      }
    }

    if (options.query.cache && parsedReq.cache !== 0) {
      console.warn(`Objection.js doesn't support query caching`);
    }

    return builder;
  }

  private async onInitMapEntityColumns() {
    this.entityColumns = (await this.fetchTableMetadata(
      this.modelClass.tableName,
    )).columns.map((column) => {
      const propertyName = (Model as any).columnNameToPropertyName(column);
      this.entityColumnsSet.add(propertyName);
      return propertyName;
    });

    this.entityPrimaryColumns = this.idColumns.map((column) =>
      (Model as any).columnNameToPropertyName(column),
    );
  }

  private prepareEntityBeforeSave(dto: T, paramsFilter: QueryFilter[]): T {
    if (!isObject(dto)) {
      return undefined;
    }

    if (hasLength(paramsFilter)) {
      for (const filter of paramsFilter) {
        dto[filter.field] = filter.value;
      }
    }

    if (!hasLength(objKeys(dto))) {
      return undefined;
    }

    return dto;
  }

  private hasColumn(column: string): boolean {
    return this.entityColumnsSet.has(column);
  }

  private getAllowedColumns(columns: string[], options: QueryOptions): string[] {
    if (!isArrayFull(options.exclude) && !isArrayFull(options.allow)) {
      return columns;
    }

    return columns.filter((column) => {
      if (isArrayFull(options.exclude) && options.exclude.includes(column)) {
        return false;
      }

      return isArrayFull(options.allow) ? options.allow.includes(column) : true;
    });
  }

  private setAndWhere(cond: QueryFilter, builder: QueryBuilder<T>) {
    this.validateHasColumn(cond.field);
    const { column, operator, value } = this.mapOperatorsToQuery(cond);

    if (operator === 'IS NULL') {
      builder.whereNull(column);
    } else if (operator === 'IS NOT NULL') {
      builder.whereNotNull(column);
    } else {
      builder.andWhere(column, operator, value);
    }
  }

  private setOrWhere(cond: QueryFilter, builder: QueryBuilder<T>) {
    this.validateHasColumn(cond.field);
    const { column, operator, value } = this.mapOperatorsToQuery(cond);

    if (operator === 'IS NULL') {
      builder.orWhereNull(column);
    } else if (operator === 'IS NOT NULL') {
      builder.orWhereNotNull(column);
    } else {
      builder.orWhere(column, operator, value);
    }
  }

  private getSelect(query: ParsedRequestParams, options: QueryOptions): string[] {
    const allowed = this.getAllowedColumns(this.entityColumns, options);

    const columns = isArrayFull(query.fields)
      ? query.fields.filter((field) => allowed.some((col) => field === col))
      : allowed;

    return [
      ...(isArrayFull(options.persist) ? options.persist : []),
      ...columns,
      ...this.entityPrimaryColumns,
    ].map((col) => this.toAliased(col));
  }

  private getSort(query: ParsedRequestParams, options: QueryOptions) {
    if (isArrayFull(query.sort)) {
      return this.mapSort(query.sort);
    }

    if (isArrayFull(options.sort)) {
      return this.mapSort(options.sort);
    }

    return [];
  }

  private mapSort(sort: QuerySort[]): { column: string; order: string }[] {
    return sort.map(({ field, order }) => {
      this.validateHasColumn(field);
      return {
        column: this.toAliased(field),
        order,
      };
    });
  }

  private mapOperatorsToQuery(
    cond: QueryFilter,
  ): { column: string; operator: string; value: any } {
    const operators = {
      eq: (column: string, val: any) => ({ column, operator: '=', value: val }),
      ne: (column: string, val: any) => ({ column, operator: '!=', value: val }),
      gt: (column: string, val: any) => ({ column, operator: '>', value: val }),
      lt: (column: string, val: any) => ({ column, operator: '<', value: val }),
      gte: (column: string, val: any) => ({ column, operator: '>=', value: val }),
      lte: (column: string, val: any) => ({ column, operator: '<=', value: val }),
      starts: (column: string, val: any) => ({
        column,
        operator: 'LIKE',
        value: `${val}%`,
      }),
      ends: (column: string, val: any) => ({
        column,
        operator: 'LIKE',
        value: `%${val}`,
      }),
      cont: (column: string, val: any) => ({
        column,
        operator: 'LIKE',
        value: `%${val}%`,
      }),
      excl: (column: string, val: any) => ({
        column,
        operator: 'NOT LIKE',
        value: `%${val}%`,
      }),
      in: (column: string, val: any) => {
        if (!isArrayFull(val)) {
          this.throwBadRequestException(`Invalid column '${column}' value`);
        }
        return {
          column,
          operator: 'IN',
          value: val,
        };
      },
      notin: (column: string, val: any) => {
        if (!isArrayFull(val)) {
          this.throwBadRequestException(`Invalid column '${column}' value`);
        }
        return {
          column,
          operator: 'NOT IN',
          value: val,
        };
      },
      isnull: (column: string) => ({
        column,
        operator: 'IS NULL',
        value: null,
      }),
      notnull: (column: string) => ({
        column,
        operator: 'IS NOT NULL',
        value: null,
      }),
      between: (column: string, val: any) => {
        if (!Array.isArray(val) || val.length !== 2) {
          this.throwBadRequestException(`Invalid column '${column}' value`);
        }

        return {
          column,
          operator: 'BETWEEN',
          value: [val[0], val[1]],
        };
      },
    };

    const normalizedColumn = cond.field.includes('.')
      ? cond.field
      : `${this.toAliased(cond.field)}`;
    return (operators[cond.operator] || operators.eq)(normalizedColumn, cond.value);
  }

  private validateHasColumn(path: string) {
    if (path.includes('.')) {
      const [relation, column, ...extra] = path.split('.');

      if (isArrayFull(extra)) {
        this.throwBadRequestException(
          `Too many nested levels! Usage: '[join=<other-relation>&]join=[<other-relation>.]<relation>&filter=<relation>.<field>||op||val'`,
        );
      }

      if (!this.hasRelation(relation)) {
        this.throwBadRequestException(`Invalid relation name '${relation}'`);
      }

      if (!this.entityRelationsHash[relation].columns.includes(column)) {
        this.throwBadRequestException(
          `Invalid column name '${column}' for relation '${relation}'`,
        );
      }
    } else {
      if (!this.hasColumn(path)) {
        this.throwBadRequestException(`Invalid column name '${path}'`);
      }
    }
  }

  private hasRelation(column: string): boolean {
    return !!this.entityRelationsHash[column];
  }

  private async onInitMapRelations() {
    // @ts-ignore
    const relations: Relation[] = Object.values((this.modelClass as any).getRelations());

    for (const relation of relations) {
      const relationTableMeta = await this.fetchTableMetadata(
        relation.relatedModelClass.tableName,
      );
      this.entityRelationsHash[relation.name] = {
        name: relation.name,
        columns: relationTableMeta.columns.map(
          (col) => (Model as any).columnNameToPropertyName(col) as string,
        ),
        referencedColumns: relation.relatedProp.props.length
          ? relation.relatedProp.props
          : relation.ownerProp.props,
      };
    }
  }

  // private getRelationMetadata(field: string) {
  //   try {
  //     const fields = field.split('.');
  //     const target = fields[fields.length - 1];
  //     const paths = fields.slice(0, fields.length - 1);
  //
  //     let relations = this.repo.metadata.relations;
  //
  //     for (const propertyName of paths) {
  //       relations = relations.find((o) => o.propertyName === propertyName)
  //         .inverseEntityMetadata.relations;
  //     }
  //
  //     const relation: RelationMetadata & { nestedRelation?: string } = relations.find(
  //       (o) => o.propertyName === target,
  //     );
  //
  //     relation.nestedRelation = `${fields[fields.length - 2]}.${target}`;
  //
  //     return relation;
  //   } catch (e) {
  //     return null;
  //   }
  // }

  private setJoin(cond: QueryJoin, joinOptions: JoinOptions, builder: QueryBuilder<T>) {
    // if (isUndefined(this.entityRelationsHash[cond.field]) && cond.field.includes('.')) {
    //   const curr = this.getRelationMetadata(cond.field);
    //   if (!curr) {
    //     this.entityRelationsHash[cond.field] = null;
    //     return true;
    //   }
    //
    //   this.entityRelationsHash[cond.field] = {
    //     name: curr.propertyName,
    //     columns: curr.inverseEntityMetadata.columns.map((col) => col.propertyName),
    //     referencedColumn: (curr.joinColumns.length
    //       ? curr.joinColumns[0]
    //       : curr.inverseRelation.joinColumns[0]
    //     ).referencedColumn.propertyName,
    //     nestedRelation: curr.nestedRelation,
    //   };
    // }

    if (cond.field && this.entityRelationsHash[cond.field] && joinOptions[cond.field]) {
      const relation = this.entityRelationsHash[cond.field];
      const options = joinOptions[cond.field];
      const allowed = this.getAllowedColumns(relation.columns, options);

      if (!allowed.length) {
        return true;
      }

      const columns = isArrayFull(cond.select)
        ? cond.select.filter((col) => allowed.includes(col))
        : allowed;

      const select = Array.from(
        new Set(
          [
            ...relation.referencedColumns,
            ...(isArrayFull(options.persist) ? options.persist : []),
            ...columns,
          ], //.map((col) => `${relation.name}.${col}`),
        ),
      );

      const relationPath = relation.nestedRelation || `${this.alias}.${relation.name}`;

      builder
        .mergeJoinEager(relation.name)
        .modifyEager(relation.name, (qb) => qb.select(select));
      // builder[relation.type](relationPath, relation.name);
      // builder.addSelect(select);
    }

    return true;
  }
}

function toChunks<T>(items: T[], size = 50): T[][] {
  const chunks = [];
  let currentChunk = [];

  items.forEach((item) => {
    if (currentChunk.length > size) {
      currentChunk = [];
      chunks.push(currentChunk);
    }

    currentChunk.push(item);
  });

  if (currentChunk.length) {
    chunks.push(currentChunk);
  }

  return chunks;
}
//
// function includes<T>(items: T[], item: T): boolean {
//   return isArrayFull(items) && items.includes(item);
// }

function decidePagination(
  parsed: ParsedRequestParams,
  options: CrudRequestOptions,
): { offset: number; limit: number } | null {
  const offsetLimit = getOffsetLimit(parsed, options);

  if (Number.isFinite(offsetLimit.offset) && Number.isFinite(offsetLimit.limit)) {
    return offsetLimit;
  }

  return null;
}

function getOffsetLimit(
  req: ParsedRequestParams,
  options: CrudRequestOptions,
): { offset: number; limit: number } {
  const limit = getLimit(req, options.query);
  const offset = getOffset(req, limit);

  return {
    limit,
    offset,
  };
}

function getOffset(query: ParsedRequestParams, limit: number): number | null {
  if (query.page && limit) {
    return limit * (query.page - 1);
  }

  if (query.offset) {
    return query.offset;
  }

  return null;
}

function getLimit(query: ParsedRequestParams, options: QueryOptions): number | null {
  if (query.limit) {
    if (options.maxLimit) {
      if (query.limit <= options.maxLimit) {
        return query.limit;
      }
      return options.maxLimit;
    }

    return query.limit;
  }

  if (options.limit) {
    if (options.maxLimit) {
      if (options.limit <= options.maxLimit) {
        return options.limit;
      }
      return options.maxLimit;
    }

    return options.limit;
  }

  return options.maxLimit ? options.maxLimit : null;
}
