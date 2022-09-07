/// <reference lib="es2021.weakref" />

export default class WeakRefImpl<T extends object> implements WeakRef<T> {

    readonly [Symbol.toStringTag] = 'WeakRef';

    static #wm = new WeakMap();

    constructor(target: T) {

        WeakRefImpl.#wm.set(this, target);
    }

    deref(): T | undefined {

        return WeakRefImpl.#wm.get(this);
    }
}
