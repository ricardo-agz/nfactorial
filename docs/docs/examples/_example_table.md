<example>
# EXAMPLE: Building a grid/table with Handsontable
You should default to using Handsontable for implementing excel-like grids/tables unless the project uses a different library or the user 
specifically specifies otherwise. 

Some examples:

```typescript
import { FC } from 'react';
import { HotTable, HotTableProps } from '@handsontable/react-wrapper';
import Handsontable from 'handsontable';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/styles/handsontable.css';
import 'handsontable/styles/ht-theme-main.css';

// register Handsontable's modules
registerAllModules();

const ExampleComponent: FC = () => {
  return (
    <HotTable
      themeName="ht-theme-main"
      data={[
        ['', 'Tesla', 'Volvo', 'Toyota', 'Ford'],
        ['2019', 10, 11, 12, 13],
        ['2020', 20, 11, 14, 13],
        ['2021', 30, 15, 12, 13],
      ]}
      rowHeaders={true}
      colHeaders={true}
      height="auto"
      autoWrapRow={true}
      autoWrapCol={true}
      licenseKey="non-commercial-and-evaluation" // for non-commercial use only
    />
  );
};

export default ExampleComponent;
```

```typescript
import { HyperFormula } from 'hyperformula';
import { HotTable } from '@handsontable/react-wrapper';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/styles/handsontable.css';
import 'handsontable/styles/ht-theme-main.css';

// register Handsontable's modules
registerAllModules();

const ExampleComponent = () => {
  const data = [
    ['150', '643', '0.32', '11', '=A1*(B1*C1)+D1'],
    // ...
    ['144', '289', '0.87', '13', '=A100*(B100*C100)+D100'],
    ['Sum', 'Average', 'Average', 'Sum', 'Sum'],
    ['=SUM(A1:A100)', '=AVERAGE(B1:B100)', '=AVERAGE(C1:C100)', '=SUM(D1:D100)', '=SUM(E1:E100)'],
  ];

  return (
    <HotTable
      themeName="ht-theme-main"
      data={data}
      formulas={{
        engine: HyperFormula,
      }}
      colHeaders={['Qty', 'Unit price', 'Discount', 'Freight', 'Total due (fx)']}
      fixedRowsBottom={2}
      stretchH="all"
      height={500}
      autoWrapRow={true}
      autoWrapCol={true}
      licenseKey="non-commercial-and-evaluation"
    />
  );
};

export default ExampleComponent;
```

```typescript
import { HotTable } from '@handsontable/react-wrapper';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/styles/handsontable.css';
import 'handsontable/styles/ht-theme-main.css';

// register Handsontable's modules
registerAllModules();

const ExampleComponent = () => {
  return (
    <HotTable
      themeName="ht-theme-main"
      data={[
        ['Tesla', 2017, 'black', 'black'],
        ['Nissan', 2018, 'blue', 'blue'],
        ['Chrysler', 2019, 'yellow', 'black'],
        ['Volvo', 2020, 'white', 'gray'],
      ]}
      colHeaders={['Car', 'Year', 'Chassis color', 'Bumper color']}
      columns={[
        {},
        { type: 'numeric' },
        {
          type: 'dropdown',
          source: ['yellow', 'red', 'orange', 'green', 'blue', 'gray', 'black', 'white'],
        },
        {
          type: 'dropdown',
          source: ['yellow', 'red', 'orange', 'green', 'blue', 'gray', 'black', 'white'],
        },
      ]}
      autoWrapRow={true}
      autoWrapCol={true}
      licenseKey="non-commercial-and-evaluation"
    />
  );
};

export default ExampleComponent;
```

**note:**
@handsontable/react-wrapper requires at least React@18 and is built with functional editors and renderers components in mind. 
ONLY use the @handsontable/react package instead if the project uses a lower version of React (not common).
</example>
