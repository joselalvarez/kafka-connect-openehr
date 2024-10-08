/*
 * Copyright (c) 2019-2024 vitasystems GmbH.
 *
 * This file is part of project EHRbase
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehrbase.openehr.sdk.dbformat.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.nedap.archie.base.OpenEHRBase;
import org.ehrbase.openehr.sdk.dbformat.DbToRmFormat;
import org.ehrbase.openehr.sdk.serialisation.jsonencoding.CanonicalJson;

public class RmDbJson {

    private RmDbJson() {}

    public static final ObjectMapper MARSHAL_OM =
            CanonicalJson.MARSHAL_OM.copy().setDefaultTyping(OpenEHRBaseTypeResolverBuilder.build());

    /**
     * Jackson type resolver builder that add Handling for {@link OpenEHRBase} types based on the <code>_type</code>
     * property.
     */
    private static class OpenEHRBaseTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

        public OpenEHRBaseTypeResolverBuilder() {
            super(ObjectMapper.DefaultTyping.NON_FINAL_AND_ENUMS, LaissezFaireSubTypeValidator.instance);
        }

        @Override
        public boolean useForType(JavaType t) {
            return OpenEHRBase.class.isAssignableFrom(t.getRawClass());
        }

        @SuppressWarnings("rawtypes")
        public static TypeResolverBuilder build() {
            return new OpenEHRBaseTypeResolverBuilder()
                    .init(JsonTypeInfo.Id.NAME, new CanonicalJson.CJOpenEHRTypeNaming())
                    .typeProperty(DbToRmFormat.TYPE_ATTRIBUTE)
                    .typeIdVisibility(true)
                    .inclusion(JsonTypeInfo.As.PROPERTY);
        }
    }
}
