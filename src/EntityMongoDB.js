import 'regenerator-runtime/runtime'; // For ES2017-await & ES2015-generators
import _ from 'lodash/fp';
import monk from 'monk';
import Sert from 'sert';

let EntityMongoDB = {};

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
    return await Entity.findOne(dbCollection, Entity, query);
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
    return Entity.findOne(dbCollection, Entity, query);
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
        return await Entity.updateOne(dbCollection, Entity, { ...updateProps, id: existing.id });
    } else {
        // Insert new entity
        return Entity.createOne(dbCollection, Entity, { ...insertOnlyProps, ...updateProps });
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
EntityMongoDB.upsertOneFP = (dbCollection, Entity, updateProps = {}, insertOnlyProps = {}) => {
    return (existing = {}) =>
        Entity.upsertOne(dbCollection, Entity, existing, updateProps, insertOnlyProps);
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
        upsertOneFP: (...args) => EntityMongoDB.upsertOneFP(dbCollection, Entity, ...args),
        deleteOne: (...args) => EntityMongoDB.deleteOne(dbCollection, ...args),
        deleteById: (...args) => EntityMongoDB.deleteById(dbCollection, ...args),
        aggregate: (...args) => EntityMongoDB.aggregate(dbCollection, ...args),
        count: (...args) => EntityMongoDB.count(dbCollection, ...args),
        distinct: (...args) => EntityMongoDB.distinct(dbCollection, ...args)
    };
};

export default EntityMongoDB;
