export const eqFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_eq`
  return {
    expression: `${fieldName} = ${valueName}`,
    values: { [valueName]: value },
    names: { [fieldName]: field },
  }
}

export const neFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_ne`
  return {
    expression: `${fieldName} <> ${valueName}`,
    names: { [fieldName]: field },
    values: { [valueName]: value },
  }
}

export const inFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_in`
  return {
    expression: `contains(${fieldName}, ${valueName})`,
    names: { [fieldName]: field },
    values: { [valueName]: value },
  }
}

export const ltFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_lt`
  return {
    expression: `${fieldName} < ${valueName}`,
    names: { [fieldName]: field },
    values: { [valueName]: value },
  }
}

export const lteFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_lte`
  return {
    expression: `${fieldName} <= ${valueName}`,
    names: { [fieldName]: field },
    values: { [valueName]: value },
  }
}

export const gtFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_gt`
  return {
    expression: `${fieldName} > ${valueName}`,
    names: { [fieldName]: field },
    values: { [valueName]: value },
  }
}

export const gteFilter = <T>(field: string) => (value: T) => {
  const fieldName = `#${field}`
  const valueName = `:${field}_gte`
  return {
    expression: `${fieldName} >= ${valueName}`,
    names: { [fieldName]: field },
    values: { [valueName]: value },
  }
}

export const exactFilters = <T>(field: string) => ({
  eq: eqFilter<T>(field),
  ne: neFilter(field),
  in: inFilter(field),
})

export const ordFilters = <T>(field: string) => ({
  eq: eqFilter<T>(field),
  lt: ltFilter(field),
  lte: lteFilter(field),
  gt: gtFilter(field),
  gte: gteFilter(field),
})
