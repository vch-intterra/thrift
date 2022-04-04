/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift;

import java.io.Serializable;

/**
 * Generic base interface for generated Thrift objects.
 *
 */
public interface TBase extends TSerializable, Serializable {

  /**
   * Get the F instance that corresponds to fieldId.
   */
  public TFieldIdEnum fieldForId(int fieldId);

  /**
   * Check if a field is currently set or unset.
   *
   * @param field
   */
  public boolean isSet(TFieldIdEnum field);

  /**
   * Get a field's value by field variable. Primitive types will be wrapped in
   * the appropriate "boxed" types.
   *
   * @param field
   */
  public Object getFieldValue(TFieldIdEnum field);

  /**
   * Set a field's value by field variable. Primitive types must be "boxed" in
   * the appropriate object wrapper type.
   *
   * @param field
   */
  public void setFieldValue(TFieldIdEnum field, Object value);

  /**
   * Create new object and copy fields from this
   * @return
   */
  public TBase deepCopy();

  /**
   * Copy fields from 'from' to this
   * @param from
   */
  public void deepCopyFields(TBase from);

  /**
   * Create new object of type T with default constructor
   * @return
   */
  public TBase newInstance();

  /**
   * Return to the state of having just been initialized, as though you had just
   * called the default constructor.
   */
  public void clear();
}
