import 'regenerator-runtime/runtime'; // For ES2017-await & ES2015-generators
import 'core-js/fn/array/is-array';
import _ from 'lodash/fp';
import monk from 'monk';
import Sert from 'sert';

let EntityMongoDB = {};

/**
 * Convert entity to mongodb-document, converting given paths to object-id
 * @param {object} entity Entity
 * @param {array} paths Paths to object-id properties
 * @return {object} MongoDB document
 */
EntityMongoDB.toDoc = (entity, paths = []) => {
    return _.omitBy(_.isUndefined, paths.reduce((entity, path) => {
        const value = _.get(path, entity);
        if (!value) {
            return entity;
        }
        const castValue = Array.isArray(value) ? value.map(monk.id) : monk.id(value);
        return _.set(path, castValue, entity);
    }, {
        _id: entity.id ? monk.id(entity.id) : undefined,
        ...entity,
        id: undefined
    }));
};

EntityMongoDB.toDocPF = paths => entity => EntityMongoDB.toDoc(entity, paths);

/**
 * Convert mongodb-document to entity structure, converting given object-id paths to strings
 * @param {object} doc MongoDB document
 * @param {array} paths Paths to object-id properties
 * @return {object} Entity
 */
EntityMongoDB.fromDoc = (doc, paths = []) => {
    return _.omitBy(_.isUndefined, paths.reduce((doc, path) => {
        const value = _.get(path, doc);
        if (!value) {
            return doc;
        }
        const castValue = Array.isArray(value) ? value.map(val => val.toString()) : value.toString();
        return _.set(path, castValue, doc);
    }, {
        id: doc._id ? doc._id.toString() : undefined,
        ...doc,
        _id: undefined
    }));
};
EntityMongoDB.fromDocPF = paths => entity => EntityMongoDB.fromDoc(entity, paths);

/**
 * Find database entities by query
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} query Query conditions (mongodb query structure)
 * @param {object} [options] Query options
 * @return {Promise}
 */
EntityMongoDB.find = async (dbCollection, Entity, query = {}, options = {}) => {
    const docs = await dbCollection.find(query, options) || [];
    return docs.map(Entity.fromDoc);
};

/**
 * Find one database entity by query
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} query Query conditions (mongodb query structure)
 * @param {object} [options] Query options
 * @return {Promise}
 */
EntityMongoDB.findOne = async (dbCollection, Entity, query = {}, options = {}) => {
    const doc = await dbCollection.findOne(query, options);
    return doc ? Entity.fromDoc(doc) : undefined;
};

/**
 * Find database entities by ids
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {array} ids Entity ids
 * @param {object} [options] Query options
 * @return {Promise}
 */
EntityMongoDB.findByIds = async (dbCollection, Entity, ids, options) => {
    const docs = await dbCollection.find({
        _id: { $in: ids.map(id => monk.id(id)) }
    }, options);
    return docs.map(Entity.fromDoc);
};

/**
 * Find one database entity by id
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {string} id Entity id
 * @param {object} [options] Query options
 * @return {Promise}
 */
EntityMongoDB.findById = async (dbCollection, Entity, id, options) => {
    const doc = await dbCollection.findOne({ _id: monk.id(id) }, options);
    return doc ? Entity.fromDoc(doc) : undefined;
};

/**
 * Create one entity (insert into database collection)
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.assertValid Assertion function for complete entity object
 * @param {function} Entity.toDoc Convert entity to mongodb-document
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} props Entity properties
 * @return {Promise}
 */
EntityMongoDB.createOne = async (dbCollection, Entity, props = {}) => {
    const cleanProps = Entity.clean(props);

    Entity.assertValid({
        ...cleanProps,
        id: '1234567890abcdef12345678' // Fake post-insert id, in case id is required
    }, {
        message: `${Entity.singularName || 'Entity'} createOne properties is invalid.`
    });

    const docProps = Entity.toDoc(cleanProps);
    const doc = await dbCollection.insert(docProps);

    return doc ? Entity.fromDoc(doc) : undefined;
};

/**
 * Update one entity (in database collection), setting only the given properties
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.assertValidPartial Assertion function for partial entity object
 * @param {function} Entity.toDoc Convert entity to mongodb-document
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} props Entity properties
 * @return {Promise}
 */
EntityMongoDB.updateOne = async (dbCollection, Entity, props = {}) => {
    const cleanProps = Entity.clean(props);

    Sert.string(cleanProps.id, 'updateOne require id property.');
    Entity.assertValidPartial(cleanProps, {
        message: `${Entity.singularName || 'Entity'} updateOne properties is invalid.`
    });

    const query = { _id: monk.id(cleanProps.id) };
    const updates = {
        $set: _.omitBy(_.isUndefined, {
            ...Entity.toDoc(cleanProps),
            id: undefined
        })
    };

    await dbCollection.update(query, updates);
    return await EntityMongoDB.findOne(dbCollection, Entity, query);
};

/**
 * Update one entity (in database collection), replacing all properties
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.assertValid Assertion function for complete entity object
 * @param {function} Entity.toDoc Convert entity to mongodb-document
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} props Entity properties
 * @return {Promise}
 */
EntityMongoDB.replaceOne = async (dbCollection, Entity, props = {}) => {
    const cleanProps = Entity.clean(props);

    Sert.string(cleanProps.id, 'replaceOne require id property.');
    Entity.assertValid(cleanProps, {
        message: `${Entity.singularName || 'Entity'} replaceOne properties is invalid.`
    });

    const query = { _id: monk.id(cleanProps.id) };

    await dbCollection.update(query, Entity.toDoc(cleanProps));
    return EntityMongoDB.findOne(dbCollection, Entity, query);
};

/**
 * Update one entity (in database collection), or insert if new
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.assertValid Assertion function for complete entity object
 * @param {function} Entity.assertValidPartial Assertion function for partial entity object
 * @param {function} Entity.toDoc Convert entity to mongodb-document
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} [existing] Existing entity to update
 * @param {object} [updateProps] Properties to set (used for both update and insert)
 * @param {object} [insertOnlyProps] Properties to set only on insert operation
 * @return {Promise}
 */
EntityMongoDB.upsertOne = async (dbCollection, Entity, existing = {}, updateProps = {}, insertOnlyProps = {}) => {
    if (existing.id !== undefined) {
        // Update existing entity
        if (Object.keys(updateProps).length === 0) {
            // No properties to update
            return existing;
        }
        return await EntityMongoDB.updateOne(dbCollection, Entity, { ...updateProps, id: existing.id });
    } else {
        // Insert new entity
        return EntityMongoDB.createOne(dbCollection, Entity, { ...insertOnlyProps, ...updateProps });
    }
};

/**
 * Generate higher order function for updating one entity by predefined props (for use in find-upsert chains)
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.assertValid Assertion function for complete entity object
 * @param {function} Entity.assertValidPartial Assertion function for partial entity object
 * @param {function} Entity.toDoc Convert entity to mongodb-document
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @param {object} [updateProps] Properties to set (used for both update and insert)
 * @param {object} [insertOnlyProps] Properties to set only on insert operation
 * @return {function} Function taking only entity prop
 */
EntityMongoDB.upsertOnePF = (dbCollection, Entity, updateProps = {}, insertOnlyProps = {}) => {
    return (existing = {}) =>
        EntityMongoDB.upsertOne(dbCollection, Entity, existing, updateProps, insertOnlyProps);
};

/**
 * Delete one entity (in database collection)
 * @param {object} dbCollection Database collection
 * @param {object} props Entity properties
 * @return {Promise}
 */
EntityMongoDB.deleteOne = (dbCollection, props = {}) => {
    Sert.string(props.id, 'deleteOne require id property.');
    return dbCollection.remove({ _id: monk.id(props.id) });
};

/**
 * Delete one entity (in database collection) by id
 * @param {object} dbCollection Database collection
 * @param {string} id Entity id
 * @return {Promise}
 */
EntityMongoDB.deleteById = (dbCollection, id) => {
    return dbCollection.remove({ _id: monk.id(id) });
};

/**
 * Run a database aggregate function
 * @param {object} dbCollection Database collection
 * @param {mixed} args Aggregate function arguments
 * @return {Promise}
 */
EntityMongoDB.aggregate = (dbCollection, ...args) => {
    return dbCollection.aggregate(...args);
};

/**
 * Run a database count function on a given database collection
 * @param {object} dbCollection Database collection
 * @param {mixed} args Count function arguments
 * @return {Promise}
 */
EntityMongoDB.count = (dbCollection, ...args) => {
    return dbCollection.count(...args);
};

/**
 * Run a database distinct function
 * @param {object} dbCollection Database collection
 * @param {mixed} args Distincs function arguments
 * @return {Promise}
 */
EntityMongoDB.distinct = (dbCollection, ...args) => {
    return dbCollection.distinct(...args);
};


// Memo.embedReferences = Factor.embedAsReferencePF('factorId', 'factor', 'tags');

/**
 * Embed a given type of entity as a reference on another object or subobject (like an entity of another type)
 * @param {object} Entity Entity functions for the referenced type of entity
 * @param {object} target Object containing the reference
 * @param {string} referencePath Path to reference id in target object
 * @param {string} [embedName] Name of embed property (reference path will be used if omitted)
 * @param {string} [objectPath] Path to object or array of objects where referenced data should be
                                embedded (for embedding on subobjects)
 * @return {object} New target object with referenced data embedded
 */
EntityMongoDB.embedAsReference = async (Entity, target, referencePath, embedName = null, objectPath = null) => {
    if (objectPath && !_.has(objectPath, target)) {
        // No data at object path - no references to embed.
        return target;
    }

    const deepTarget = _.get(objectPath, target) || target;

    const withEmbedsArray = await Promise.all(_.castArray(deepTarget)
        .map(async deepTarget => {
            const reference = _.get(referencePath, deepTarget);

            if (!reference) {
                // No reference on this target object
                return deepTarget;
            }

            const embeds = Array.isArray(reference) ?
                await Entity.findByIds(reference)
                :
                await Entity.findById(reference);

            return {
                ...deepTarget,
                _embedded: {
                    ...(deepTarget._embedded || _.stubObject),
                    [embedName || referencePath]: embeds
                }
            };
        })
    );

    const withEmbeds = Array.isArray(deepTarget) ? withEmbedsArray : _.head(withEmbedsArray);
    const ret = objectPath ?
        _.set(objectPath, withEmbeds, target)
        :
        withEmbeds;

    return ret;
};


/**
 * Composition of all functions as partials with collection/Entity applied
 * @param {object} dbCollection Database collection
 * @param {object} Entity Entity function set
 * @param {function} Entity.assertValid Assertion function for complete entity object
 * @param {function} Entity.assertValidPartial Assertion function for partial entity object
 * @param {function} Entity.toDoc Convert entity to mongodb-document
 * @param {function} Entity.fromDoc Convert mongodb-document to entity
 * @return {object} Function container
 */
EntityMongoDB.all = (dbCollection, Entity) => {
    return {
        find: (...args) => EntityMongoDB.find(dbCollection, Entity, ...args),
        findOne: (...args) => EntityMongoDB.findOne(dbCollection, Entity, ...args),
        findByIds: (...args) => EntityMongoDB.findByIds(dbCollection, Entity, ...args),
        findById: (...args) => EntityMongoDB.findById(dbCollection, Entity, ...args),
        createOne: (...args) => EntityMongoDB.createOne(dbCollection, Entity, ...args),
        updateOne: (...args) => EntityMongoDB.updateOne(dbCollection, Entity, ...args),
        replaceOne: (...args) => EntityMongoDB.replaceOne(dbCollection, Entity, ...args),
        upsertOne: (...args) => EntityMongoDB.upsertOne(dbCollection, Entity, ...args),
        upsertOnePF: (...args) => EntityMongoDB.upsertOnePF(dbCollection, Entity, ...args),
        deleteOne: (...args) => EntityMongoDB.deleteOne(dbCollection, ...args),
        deleteById: (...args) => EntityMongoDB.deleteById(dbCollection, ...args),
        aggregate: (...args) => EntityMongoDB.aggregate(dbCollection, ...args),
        count: (...args) => EntityMongoDB.count(dbCollection, ...args),
        distinct: (...args) => EntityMongoDB.distinct(dbCollection, ...args),

        embedAsReference: (...args) => EntityMongoDB.embedAsReference(Entity, ...args),
        embedAsReferencePF: (...args) => target => EntityMongoDB.embedAsReference(Entity, target, ...args),
        embedAsReferenceAPF: (...args) => async target => await EntityMongoDB.embedAsReference(Entity, await target, ...args)
    };
};

export default EntityMongoDB;
